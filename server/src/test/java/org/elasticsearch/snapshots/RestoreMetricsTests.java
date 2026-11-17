/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreFailureReason;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link RestoreMetrics}: the counters and histograms that {@link RestoreService} records when a restore starts and completes, and
 * the gauges that are computed from real cluster states held by a real cluster service, in which shards of a restore are queued, recovering,
 * retried, restored or failed.
 */
public class RestoreMetricsTests extends ESTestCase {

    private static final int NUMBER_OF_SHARDS = 3;
    private static final ProjectId PROJECT_ID = ProjectId.DEFAULT;

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private RecordingMeterRegistry meterRegistry;
    private RestoreMetrics restoreMetrics;
    private long nowMillis;

    private IndexMetadata indexMetadata;
    private Snapshot snapshot;
    private String restoreUuid;
    private boolean reportShardRestoring;

    @Before
    public void createRestoreMetrics() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        meterRegistry = new RecordingMeterRegistry();
        nowMillis = randomLongBetween(1_000_000, 2_000_000_000_000L);
        restoreMetrics = new RestoreMetrics(meterRegistry, clusterService, () -> nowMillis);
        indexMetadata = IndexMetadata.builder(randomIndexName())
            .settings(indexSettings(IndexVersion.current(), randomUUID(), NUMBER_OF_SHARDS, 0))
            .build();
        snapshot = new Snapshot(PROJECT_ID, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));
        restoreUuid = randomUUID();
        reportShardRestoring = randomBoolean();
        // the gauges only report on the master
        ClusterServiceUtils.setState(clusterService, state(true, allQueued(), RestoreInProgress.EMPTY));
        restoreMetrics.start();
    }

    @After
    public void closeClusterService() {
        restoreMetrics.close();
        clusterService.close();
        terminate(threadPool);
    }

    /**
     * The gauges are registered when the node starts this component and released when it closes, like the snapshot gauges, so that they
     * are never polled against a cluster service that has not started or has closed.
     */
    public void testGaugesRegisteredOnlyWhileStarted() {
        final RecordingMeterRegistry registry = new RecordingMeterRegistry();
        final RestoreMetrics metrics = new RestoreMetrics(registry, clusterService, () -> nowMillis);
        assertThat(registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE), empty());
        metrics.start();
        assertThat(
            Set.copyOf(registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE)),
            equalTo(
                Set.of(
                    RestoreMetrics.RESTORES_CURRENT,
                    RestoreMetrics.RESTORES_INDICES_CURRENT,
                    RestoreMetrics.RESTORES_SHARDS_BY_STATE_CURRENT,
                    RestoreMetrics.RESTORES_SHARDS_RETRYING_CURRENT,
                    RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT
                )
            )
        );
        metrics.close();
        assertThat(registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE), empty());
    }

    /**
     * A started restore is counted with whether it reports its shards. A completed one is counted with its outcome, which is partial when
     * only some shards failed, with its sizes, and with how each of its shards ended.
     */
    public void testRestoreStartedAndCompleted() {
        restoreMetrics.restoreStarted(entry(RestoreInProgress.State.INIT, inProgress(), inProgress(), inProgress()));
        assertThat(counterTotal(RestoreMetrics.RESTORES_STARTED_TOTAL, reportShardRestoringAttribute()), equalTo(1L));

        restoreMetrics.restoreCompleted(
            entry(
                RestoreInProgress.State.FAILURE,
                succeeded(),
                ShardRestoreStatus.failed(null, ShardRestoreFailureReason.NO_ELIGIBLE_NODE, "no node"),
                succeeded()
            )
        );
        final Map<String, Object> completedAttributes = new HashMap<>(reportShardRestoringAttribute());
        completedAttributes.put(RestoreMetrics.OUTCOME_ATTRIBUTE, "partial");
        assertThat(counterTotal(RestoreMetrics.RESTORES_COMPLETED_TOTAL, completedAttributes), equalTo(1L));
        assertThat(counterTotal(RestoreMetrics.RESTORES_SHARDS_COMPLETED_TOTAL, Map.of()), equalTo(2L));
        assertThat(
            counterTotal(RestoreMetrics.RESTORES_SHARDS_FAILED_TOTAL, Map.of(RestoreMetrics.FAILURE_REASON_ATTRIBUTE, "no_eligible_node")),
            equalTo(1L)
        );
        assertHistogram(RestoreMetrics.RESTORES_INDICES_HISTOGRAM, 1L, "partial");
        assertHistogram(RestoreMetrics.RESTORES_SHARDS_HISTOGRAM, NUMBER_OF_SHARDS, "partial");
    }

    /**
     * A restore in which every shard ends the same way has the matching outcome, and a failure that a node recorded without classifying
     * it is counted as unknown.
     */
    public void testOutcomeAndUnclassifiedFailure() {
        final boolean allSucceed = randomBoolean();
        final ShardRestoreStatus[] statuses = new ShardRestoreStatus[NUMBER_OF_SHARDS];
        for (int i = 0; i < NUMBER_OF_SHARDS; i++) {
            statuses[i] = allSucceed
                ? succeeded()
                : new ShardRestoreStatus("node", RestoreInProgress.State.FAILURE, "failed on an older node");
        }
        restoreMetrics.restoreCompleted(entry(allSucceed ? RestoreInProgress.State.SUCCESS : RestoreInProgress.State.FAILURE, statuses));

        assertThat(
            counterTotal(
                RestoreMetrics.RESTORES_COMPLETED_TOTAL,
                Map.of(RestoreMetrics.OUTCOME_ATTRIBUTE, allSucceed ? "success" : "failure")
            ),
            equalTo(1L)
        );
        assertThat(
            counterTotal(RestoreMetrics.RESTORES_SHARDS_COMPLETED_TOTAL, Map.of()),
            equalTo(allSucceed ? (long) NUMBER_OF_SHARDS : 0L)
        );
        assertThat(
            counterTotal(
                RestoreMetrics.RESTORES_SHARDS_FAILED_TOTAL,
                Map.of(RestoreMetrics.FAILURE_REASON_ATTRIBUTE, RestoreMetrics.UNKNOWN_FAILURE_REASON)
            ),
            equalTo(allSucceed ? 0L : NUMBER_OF_SHARDS)
        );
    }

    /**
     * The gauges describe the restores in progress in the cluster service's state: a queued shard is told from a recovering one, a shard
     * that has failed and is being retried is counted, the oldest restoring shard's age is measured from when it became unassigned for its
     * current attempt, and a completed restore that is only waiting to be removed is left out.
     */
    public void testGauges() {
        final long oldestAgeMillis = randomLongBetween(1_000, 1_000_000);
        final RestoreInProgress.Entry inProgress = entry(RestoreInProgress.State.STARTED, inProgress(), inProgress(), succeeded());
        // the completed restore's shard has been unassigned for longer, but must not count
        final RestoreInProgress.Entry awaitingRemoval = new RestoreInProgress.Entry(
            randomUUID(),
            snapshot,
            RestoreInProgress.State.SUCCESS,
            false,
            List.of(indexMetadata.getIndex().getName()),
            Map.of(shardId(2), succeeded()),
            reportShardRestoring
        );
        ClusterServiceUtils.setState(
            clusterService,
            state(
                true,
                List.of(
                    queued(0, between(1, 5), nowMillis - oldestAgeMillis),
                    recovering(1, 0, nowMillis - randomLongBetween(0, oldestAgeMillis)),
                    startedPrimary(2)
                ),
                new RestoreInProgress.Builder().add(inProgress).add(awaitingRemoval).build()
            )
        );
        collect();

        assertThat(gauge(RestoreMetrics.RESTORES_CURRENT, reportShardRestoringAttribute()), equalTo(1L));
        assertThat(
            gauge(RestoreMetrics.RESTORES_CURRENT, Map.of(RestoreMetrics.REPORT_SHARD_RESTORING_ATTRIBUTE, reportShardRestoring == false)),
            equalTo(0L)
        );
        assertThat(gauge(RestoreMetrics.RESTORES_INDICES_CURRENT, Map.of()), equalTo(1L));
        assertThat(shardsInState("queued"), equalTo(1L));
        assertThat(shardsInState("recovering"), equalTo(1L));
        assertThat(shardsInState("succeeded"), equalTo(1L));
        assertThat(shardsInState("failed"), equalTo(0L));
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_RETRYING_CURRENT, Map.of()), equalTo(1L));
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT, Map.of()), equalTo(oldestAgeMillis));

        // the age grows with the clock while the cluster state stays the same
        final long elapsedMillis = randomLongBetween(1, 100_000);
        nowMillis += elapsedMillis;
        collect();
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT, Map.of()), equalTo(oldestAgeMillis + elapsedMillis));
    }

    /**
     * The gauges follow the cluster state as it changes: once the restoring shards finish, nothing is queued, recovering or retrying, and
     * the oldest age is zero. An unassigned time ahead of this node's clock, which another master may have set, never gives a negative age.
     */
    public void testGaugesFollowClusterState() {
        final RestoreInProgress.Entry restoring = entry(RestoreInProgress.State.STARTED, inProgress(), inProgress(), inProgress());
        ClusterServiceUtils.setState(
            clusterService,
            state(
                true,
                List.of(queued(0, 1, nowMillis + randomLongBetween(1, 1_000)), queued(1, 0, nowMillis), queued(2, 0, nowMillis)),
                new RestoreInProgress.Builder().add(restoring).build()
            )
        );
        collect();
        assertThat(shardsInState("queued"), equalTo((long) NUMBER_OF_SHARDS));
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_RETRYING_CURRENT, Map.of()), equalTo(1L));
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT, Map.of()), equalTo(0L));

        final RestoreInProgress.Entry finished = entry(RestoreInProgress.State.STARTED, succeeded(), succeeded(), succeeded());
        ClusterServiceUtils.setState(
            clusterService,
            state(
                true,
                List.of(startedPrimary(0), startedPrimary(1), startedPrimary(2)),
                new RestoreInProgress.Builder().add(finished).build()
            )
        );
        nowMillis += randomLongBetween(0, 100_000);
        collect();
        assertThat(shardsInState("queued"), equalTo(0L));
        assertThat(shardsInState("succeeded"), equalTo((long) NUMBER_OF_SHARDS));
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_RETRYING_CURRENT, Map.of()), equalTo(0L));
        assertThat(gauge(RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT, Map.of()), equalTo(0L));
    }

    /**
     * A node that is not the master reports no gauges, so that the cluster reports each restore once.
     */
    public void testNoGaugesWhenNotMaster() {
        ClusterServiceUtils.setState(
            clusterService,
            state(false, allQueued(), new RestoreInProgress.Builder().add(entry(RestoreInProgress.State.STARTED, inProgress())).build())
        );
        collect();
        assertThat(meterRegistry.getRecorder().getAllMeasurements(), empty());
    }

    private void collect() {
        meterRegistry.getRecorder().resetCalls();
        meterRegistry.getRecorder().collect();
    }

    private long shardsInState(String state) {
        return gauge(RestoreMetrics.RESTORES_SHARDS_BY_STATE_CURRENT, Map.of(RestoreMetrics.SHARD_STATE_ATTRIBUTE, state));
    }

    private Map<String, Object> reportShardRestoringAttribute() {
        return Map.of(RestoreMetrics.REPORT_SHARD_RESTORING_ATTRIBUTE, reportShardRestoring);
    }

    private List<Measurement> measurements(InstrumentType type, String name) {
        return meterRegistry.getRecorder().getMeasurements(type, name);
    }

    /** The sum of the counter's increments whose attributes include all of {@code attributes}. */
    private long counterTotal(String name, Map<String, Object> attributes) {
        return measurements(InstrumentType.LONG_COUNTER, name).stream()
            .filter(m -> m.attributes().entrySet().containsAll(attributes.entrySet()))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    /** The one value the gauge recorded with exactly {@code attributes}. */
    private long gauge(String name, Map<String, Object> attributes) {
        final List<Measurement> recorded = measurements(InstrumentType.LONG_ASYNC_GAUGE, name);
        final List<Measurement> matching = recorded.stream().filter(m -> m.attributes().equals(attributes)).toList();
        assertThat(name + " " + attributes + " in " + recorded, matching.size(), equalTo(1));
        return matching.getFirst().getLong();
    }

    private void assertHistogram(String name, long expectedValue, String outcome) {
        final List<Measurement> recorded = measurements(InstrumentType.LONG_HISTOGRAM, name);
        assertThat(recorded.size(), equalTo(1));
        assertThat(recorded.getFirst().getLong(), equalTo(expectedValue));
        assertThat(recorded.getFirst().attributes(), equalTo(Map.of(RestoreMetrics.OUTCOME_ATTRIBUTE, outcome)));
    }

    private ClusterState state(boolean localNodeIsMaster, List<ShardRouting> primaries, RestoreInProgress restoreInProgress) {
        final DiscoveryNode localNode = clusterService.localNode();
        final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        primaries.forEach(indexRoutingTable::addShard);
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(localNode)
                    .localNodeId(localNode.getId())
                    .masterNodeId(localNodeIsMaster ? localNode.getId() : null)
            )
            .metadata(Metadata.builder().put(ProjectMetadata.builder(PROJECT_ID).put(indexMetadata, false)))
            .putRoutingTable(PROJECT_ID, RoutingTable.builder().add(indexRoutingTable).build())
            .putCustom(RestoreInProgress.TYPE, restoreInProgress)
            .build();
    }

    /** The restore's entry, whose shard {@code i} has {@code statuses[i]}. */
    private RestoreInProgress.Entry entry(RestoreInProgress.State state, ShardRestoreStatus... statuses) {
        final Map<ShardId, ShardRestoreStatus> shards = new HashMap<>();
        for (int i = 0; i < statuses.length; i++) {
            shards.put(shardId(i), statuses[i]);
        }
        return new RestoreInProgress.Entry(
            restoreUuid,
            snapshot,
            state,
            false,
            List.of(indexMetadata.getIndex().getName()),
            Map.copyOf(shards),
            reportShardRestoring
        );
    }

    private static ShardRestoreStatus inProgress() {
        return new ShardRestoreStatus("node");
    }

    private static ShardRestoreStatus succeeded() {
        return new ShardRestoreStatus("node", RestoreInProgress.State.SUCCESS);
    }

    private ShardId shardId(int shard) {
        return new ShardId(indexMetadata.getIndex(), shard);
    }

    private List<ShardRouting> allQueued() {
        return List.of(queued(0, 0, nowMillis), queued(1, 0, nowMillis), queued(2, 0, nowMillis));
    }

    /**
     * An unassigned primary waiting to restore, which became unassigned at {@code unassignedTimeMillis} after failing to recover
     * {@code failures} times so far.
     */
    private ShardRouting queued(int shard, int failures, long unassignedTimeMillis) {
        return TestShardRouting.shardRoutingBuilder(shardId(shard), null, true, ShardRoutingState.UNASSIGNED)
            .withRecoverySource(snapshotRecoverySource())
            .withUnassignedInfo(unassignedInfo(failures, unassignedTimeMillis))
            .build();
    }

    /** A primary recovering from the snapshot, which became unassigned at {@code unassignedTimeMillis} after {@code failures} failures. */
    private ShardRouting recovering(int shard, int failures, long unassignedTimeMillis) {
        return TestShardRouting.shardRoutingBuilder(
            shardId(shard),
            clusterService.localNode().getId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(snapshotRecoverySource()).withUnassignedInfo(unassignedInfo(failures, unassignedTimeMillis)).build();
    }

    private ShardRouting startedPrimary(int shard) {
        return TestShardRouting.newShardRouting(shardId(shard), clusterService.localNode().getId(), true, ShardRoutingState.STARTED);
    }

    private RecoverySource.SnapshotRecoverySource snapshotRecoverySource() {
        return new RecoverySource.SnapshotRecoverySource(
            restoreUuid,
            snapshot,
            IndexVersion.current(),
            new IndexId(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID())
        );
    }

    private static UnassignedInfo unassignedInfo(int failures, long unassignedTimeMillis) {
        return new UnassignedInfo(
            failures == 0 ? UnassignedInfo.Reason.NEW_INDEX_RESTORED : UnassignedInfo.Reason.ALLOCATION_FAILED,
            failures == 0 ? "restoring" : "failed to recover",
            null,
            failures,
            System.nanoTime(),
            unassignedTimeMillis,
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Set.of(),
            null
        );
    }
}
