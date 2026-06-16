# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.util import wait_until_with_progress_check

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.admin import Admin
from rptest.services.redpanda import SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.scale_parameters import ScaleParameters


class CloudTopicsColdReadScaleTest(PreallocNodesTest):
    """Scale gate for the cloud-topics read-under-produce path.

    A steady producer keeps writing a cloud topic while a large backlog is
    drained from object storage. Cold fetches (consumer_fetch) and L0 uploads
    (producer_upload) then contend for the per-shard S3 connection pool -- the
    regime the cloud_io scheduler exists to arbitrate. The test asserts the
    path stays healthy under that contention: produce keeps making progress
    (no stall), the cold reader drains the backlog, and the cluster ends
    healthy.

    Scope: this is a coarse CDT regression gate, NOT the scheduler
    reservation-vs-passthrough A/B. The precise A/B lives in the locked
    bench-runner tier-9 configs; the floor mechanism is unit-tested in
    cloud_io/tests/scheduler_test.cc. This runs at CDT scale on purpose --
    the reads only reach the connection pool once the backlog exceeds local
    capacity, which docker can't manufacture.

    Cold reads: cloud topics are cloud-first (~zero local retention), so the
    backlog is served warm only out of the in-memory batch cache. The test sets
    disable_batch_cache so every read of the backlog misses memory and fetches
    cold from object storage, hitting the connection pool -- the regime under
    test. CALIBRATION: confirm on the first CDT run that object-storage GETs
    climb during the drain; if reads still look warm, raise BACKLOG_SEC.
    """

    topics = ()

    NUM_BROKERS = 9
    NUM_CLIENT_NODES = 3
    MSG_SIZE = 16 * 1024

    # High partition density so the cold drain issues many concurrent fetches
    # across shards (concurrency is what pressures the connection pool). Capped
    # by the cluster's partition limit at runtime.
    MAX_PARTITIONS = 2000

    # A moderate per-shard pool (vs the ~20 shipped default) so the cold drain
    # can actually contend for slots within a bounded test; the point is to
    # exercise the contended path, not to replicate production sizing.
    POOL_CONNECTIONS = 10

    # Steady produce -- the protected write path. Modest vs cluster capacity;
    # the cold drain (unbounded) is what pressures the pool.
    PRODUCE_RATE_BPS = 50 * 1024 * 1024
    # How long to build the cold backlog before the drain starts. CALIBRATION:
    # the resulting volume must exceed local capacity to read cold (see class
    # docstring).
    BACKLOG_SEC = 5 * 60
    # The cold drain must finish within this once it starts.
    DRAIN_TIMEOUT_SEC = 20 * 60
    SAMPLE_INTERVAL_SEC = 30

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=self.POOL_CONNECTIONS,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        extra_rp_conf = {
            "enable_cluster_metadata_upload_loop": False,
            # Cloud topics keep ~zero local retention, so the batch cache is the
            # only layer that would serve the backlog warm. Disable it so every
            # backlog read fetches cold from object storage and pressures the
            # connection pool -- the regime under test.
            "disable_batch_cache": True,
        }
        super().__init__(
            test_context,
            num_brokers=self.NUM_BROKERS,
            node_prealloc_count=self.NUM_CLIENT_NODES,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _cluster_healthy(self) -> bool:
        overview = self.admin.get_cluster_health_overview()
        healthy = overview.get("is_healthy", False)
        if not healthy:
            self.logger.warning(f"Cluster unhealthy: {overview}")
        return healthy

    @cluster(num_nodes=12)
    @skip_debug_mode
    def test_cold_read_under_produce(self):
        scale = ScaleParameters(self.redpanda, replication_factor=3)
        partitions = min(self.MAX_PARTITIONS, max(1, scale.partition_limit))

        topic = "cloud_topics_cold_read"
        self.rpk.create_topic(
            topic=topic,
            partitions=partitions,
            replicas=3,
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
        )
        self.logger.info(
            f"Created cloud topic {topic} with {partitions} partitions "
            f"(partition_limit={scale.partition_limit})"
        )

        rate = int(min(self.PRODUCE_RATE_BPS, scale.expect_bandwidth))
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=self.MSG_SIZE,
            msg_count=100_000_000,  # ceiling; the producer is stopped by time
            rate_limit_bps=rate,
            custom_node=[self.preallocated_nodes[0]],
            tolerate_failed_produce=True,
        )
        # One cold pass from offset 0 -> drains the backlog from object storage
        # while produce continues. Unbounded, so it pressures the pool as hard
        # as it can.
        reader = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            topic,
            loop=False,
            nodes=[self.preallocated_nodes[1]],
        )

        producer.start()
        try:
            # ── Build a cold backlog ─────────────────────────────────────
            backlog_target = int(rate * self.BACKLOG_SEC / self.MSG_SIZE)
            self.logger.info(
                f"Building backlog: ~{backlog_target} msgs "
                f"(~{rate * self.BACKLOG_SEC / 1024 ** 3:.0f} GiB) at {rate} B/s"
            )
            producer.wait_for_acks(
                count=backlog_target,
                timeout_sec=self.BACKLOG_SEC * 2,
                backoff_sec=5,
            )

            # ── Drain the cold backlog while produce continues ───────────
            backlog_at_read = producer.produce_status.acked
            self.logger.info(
                f"Backlog built ({backlog_at_read} acked); starting cold drain "
                f"with produce still running"
            )
            reader.start()

            # Produce must keep advancing while the cold reader runs: if cold
            # fetches starve the produce path, acked stops climbing and the
            # progress check trips. The drain finishing is the loop's exit.
            wait_until_with_progress_check(
                check=lambda: producer.produce_status.acked,
                condition=lambda: not reader.consumer_status.active,
                timeout_sec=self.DRAIN_TIMEOUT_SEC,
                progress_sec=self.SAMPLE_INTERVAL_SEC,
                backoff_sec=5,
                err_msg="Produce stalled while the cold backlog was draining",
                logger=self.logger,
            )
        finally:
            reader.stop()
            producer.stop()

        # The reader must have drained at least the backlog that existed when
        # it started; failing this means the cold read stalled/cascaded.
        read = reader.consumer_status.validator.valid_reads
        self.logger.info(
            f"Cold drain done: read={read}, produced={producer.produce_status.acked}"
        )
        assert read >= backlog_at_read, (
            f"cold reader did not drain the backlog: read {read} < backlog "
            f"{backlog_at_read} (cold-read stall or cascade?)"
        )

        wait_until(
            self._cluster_healthy,
            timeout_sec=60,
            backoff_sec=5,
            err_msg="Cluster has unavailable partitions after the test",
        )
        self.logger.info("Cluster healthy -- test passed")
