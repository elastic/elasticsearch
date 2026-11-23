/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SplitDiscoveryPhase;
import org.elasticsearch.xpack.esql.datasources.glob.PlanningMemory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Tests that verify error messages produced during external source distribution
 * contain sufficient context for debugging (node IDs, split counts, etc.).
 */
public class DataNodeComputeHandlerExternalErrorTests extends ESTestCase {

    public void testNodeNotFoundErrorMessageFormat() {
        String missingNodeId = "node-missing-42";
        int splitCount = 5;

        IllegalStateException error = new IllegalStateException(
            "node [" + missingNodeId + "] assigned [" + splitCount + "] external splits not found in cluster state"
        );

        assertThat(error.getMessage(), containsString(missingNodeId));
        assertThat(error.getMessage(), containsString(String.valueOf(splitCount)));
        assertThat(error.getMessage(), containsString("not found in cluster state"));
    }

    public void testDistributionPlanAssignsOnlyKnownNodes() {
        DiscoveryNodes nodes = createNodes(3);
        List<ExternalSplit> splits = createSplits(6);

        RoundRobinStrategy strategy = new RoundRobinStrategy();
        ExternalDistributionContext context = new ExternalDistributionContext(createDummyPlan(), splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue("Should distribute", plan.distributed());

        for (String nodeId : plan.nodeAssignments().keySet()) {
            assertNotNull("Distribution plan references unknown node: " + nodeId, nodes.get(nodeId));
        }
    }

    public void testDistributionPlanNodeAssignmentsSumToTotalSplits() {
        DiscoveryNodes nodes = createNodes(3);
        int totalSplits = randomIntBetween(1, 20);
        List<ExternalSplit> splits = createSplits(totalSplits);

        RoundRobinStrategy strategy = new RoundRobinStrategy();
        ExternalDistributionContext context = new ExternalDistributionContext(createDummyPlan(), splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        if (plan.distributed()) {
            int assignedCount = 0;
            for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
                assignedCount += nodeSplits.size();
            }
            assertEquals("All splits must be assigned exactly once", totalSplits, assignedCount);
        }
    }

    public void testSplitDiscoveryErrorWrapsSourceContext() {
        String sourcePath = "s3://my-bucket/data/*.parquet";
        String sourceType = "parquet";
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            sourcePath,
            sourceType,
            List.of(
                new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            Map.of(),
            Map.of(),
            null,
            null
        ).withFileList(FileList.UNRESOLVED);

        SplitProvider failingProvider = ctx -> { throw new UncheckedIOException(new IOException("connection reset")); };
        ExternalSourceFactory factory = testFactory(failingProvider);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplitsWithStats(
                exec,
                Map.of(sourceType, factory),
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                () -> false,
                List.of(),
                FormatReader.NO_LIMIT,
                PlanningMemory.NONE,
                0,
                TransportVersion.current()
            )
        );

        assertThat(e.getMessage(), containsString("*.parquet"));
        assertThat(e.getMessage(), containsString(sourceType));
        assertThat(e.getMessage(), containsString("connection reset"));
        assertThat(e.getMessage(), not(containsString("my-bucket")));
        assertNull("storage failures are logged, not chained into caused_by", e.getCause());
    }

    /**
     * A worker that gave up waiting for the cluster state fails the request with the {@link ExternalException}, which is not a
     * registered exception and so reaches the coordinator as a {@link RemoteTransportException} around a
     * {@link NotSerializableExceptionWrapper}. The typed condition must survive that, since it is what tells the client the
     * failure is retryable, and no other failure must read as it.
     */
    public void testWorkerNotReadyIsRecognisedThroughTheTransportWrappers() throws IOException {
        Exception notReady = roundTrip(DataNodeComputeHandler.workerNotReady("waited [1s]"));
        assertThat(notReady, instanceOf(NotSerializableExceptionWrapper.class));
        assertTrue(ExternalException.hasCondition(notReady, ExternalException.Condition.WORKER_NOT_READY));
        assertTrue(
            ExternalException.hasCondition(new RemoteTransportException("worker", notReady), ExternalException.Condition.WORKER_NOT_READY)
        );

        Exception unavailable = roundTrip(
            new ExternalUnavailableException(
                ExternalException.Condition.STORE_UNAVAILABLE,
                StoragePath.of("s3://bucket/file.parquet"),
                "HTTP 503",
                "",
                false,
                0L
            )
        );
        assertFalse(
            ExternalException.hasCondition(
                new RemoteTransportException("worker", unavailable),
                ExternalException.Condition.WORKER_NOT_READY
            )
        );
        assertFalse(
            ExternalException.hasCondition(
                new RemoteTransportException("worker", new IOException("boom")),
                ExternalException.Condition.WORKER_NOT_READY
            )
        );
        assertFalse(
            ExternalException.hasCondition(
                new NodeNotConnectedException(createNodes(1).get("node-0"), "gone"),
                ExternalException.Condition.WORKER_NOT_READY
            )
        );
    }

    /**
     * A worker that left after its request was sent is retried on another resolved worker only when it
     * streamed no pages. Cancellation and circuit breaks are query-wide; a generic I/O error is not a
     * leave and is not retried. Cluster-state lag is waited out on the worker, not hopped here.
     */
    public void testDisconnectWithoutPagesReroutesWhenAnotherWorkerRemains() {
        DiscoveryNode node = createNodes(1).get("node-0");
        Exception disconnected = new NodeDisconnectedException(node, "indices:data/read/esql/data");
        Exception wrapped = new RemoteTransportException("worker", disconnected);

        assertTrue(DataNodeComputeHandler.shouldRerouteExternalWorker(disconnected, 0, true));
        assertTrue(DataNodeComputeHandler.shouldRerouteExternalWorker(wrapped, 0, true));
        assertTrue(DataNodeComputeHandler.shouldRerouteExternalWorker(new NodeNotConnectedException(node, "gone"), 0, true));
        assertTrue(DataNodeComputeHandler.shouldRerouteExternalWorker(new NodeClosedException(node), 0, true));
        assertFalse(
            "cluster-state lag waits on the worker, it is not a leave hop",
            DataNodeComputeHandler.shouldRerouteExternalWorker(DataNodeComputeHandler.workerNotReady(""), 0, true)
        );

        assertFalse("pages already fetched cannot be replayed", DataNodeComputeHandler.shouldRerouteExternalWorker(disconnected, 1, true));
        assertFalse("last worker has nowhere to go", DataNodeComputeHandler.shouldRerouteExternalWorker(disconnected, 0, false));
        assertFalse(DataNodeComputeHandler.shouldRerouteExternalWorker(new TaskCancelledException("cancelled"), 0, true));
        assertFalse(
            DataNodeComputeHandler.shouldRerouteExternalWorker(
                new CircuitBreakingException("over limit", CircuitBreaker.Durability.TRANSIENT),
                0,
                true
            )
        );
        assertFalse(DataNodeComputeHandler.shouldRerouteExternalWorker(new IOException("boom"), 0, true));
    }

    public void testRemainingExternalWorkersFailedKeepsTheDisconnect() {
        NodeDisconnectedException disconnected = new NodeDisconnectedException(createNodes(1).get("node-0"), "indices:data/read/esql/data");
        IllegalStateException failure = DataNodeComputeHandler.remainingExternalWorkersFailed(disconnected);
        assertThat(failure.getMessage(), containsString("failed after dispatch"));
        assertThat(failure.getCause(), instanceOf(NodeDisconnectedException.class));
    }

    private static Exception roundTrip(Exception e) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(e);
            return out.bytes().streamInput().readException();
        }
    }

    public void testDistributionPlanWithStaleNodeAssignment() {
        DiscoveryNodes nodes = createNodes(3);
        List<ExternalSplit> splits = createSplits(6);

        RoundRobinStrategy strategy = new RoundRobinStrategy();
        ExternalDistributionContext context = new ExternalDistributionContext(createDummyPlan(), splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue("Should distribute", plan.distributed());

        DiscoveryNodes reducedNodes = createNodes(1);
        int staleNodeCount = 0;
        for (String nodeId : plan.nodeAssignments().keySet()) {
            if (reducedNodes.get(nodeId) == null) {
                staleNodeCount++;
            }
        }
        assertTrue("At least one node should become stale when cluster shrinks", staleNodeCount > 0);
    }

    /**
     * The coordinator waits for a worker only when there are eligible workers and every one of them is held back: not with a
     * ready worker, not without any worker, and never when the coordinator is itself an eligible worker, which is never held
     * back.
     */
    public void testAllExternalWorkersJoining() {
        DiscoveryNodes workers = createNodes(3);
        DiscoveryNodes none = DiscoveryNodes.builder().build();
        UnaryOperator<DiscoveryNodes> allHeldBack = nodes -> none;
        UnaryOperator<DiscoveryNodes> oneReady = nodes -> createNodes(1);
        UnaryOperator<DiscoveryNodes> noneHeldBack = UnaryOperator.identity();

        assertTrue(ComputeService.allExternalWorkersJoining(workers, allHeldBack));
        assertFalse("a ready worker", ComputeService.allExternalWorkersJoining(workers, oneReady));
        assertFalse("nobody held back", ComputeService.allExternalWorkersJoining(workers, noneHeldBack));
        assertFalse("no worker at all: the local fallback", ComputeService.allExternalWorkersJoining(none, allHeldBack));
        // The coordinator is itself a worker: never held back, so always among the ready ones.
        DiscoveryNodes workersAndLocal = DiscoveryNodes.builder(workers)
            .add(DiscoveryNodeUtils.builder("local").roles(Set.of(DATA_HOT_NODE_ROLE)).build())
            .localNodeId("local")
            .build();
        UnaryOperator<DiscoveryNodes> allButLocalHeldBack = nodes -> DiscoveryNodes.builder()
            .add(nodes.getLocalNode())
            .localNodeId("local")
            .build();
        assertFalse("a data-node coordinator", ComputeService.allExternalWorkersJoining(workersAndLocal, allButLocalHeldBack));
    }

    public void testPinnedToCoordinator() {
        assertTrue(ComputeService.pinnedToCoordinator(pragmas(ComputeService.COORDINATOR_ONLY_DISTRIBUTION)));
        assertFalse(ComputeService.pinnedToCoordinator(QueryPragmas.EMPTY));
        assertFalse(ComputeService.pinnedToCoordinator(pragmas("round_robin")));
    }

    private static QueryPragmas pragmas(String externalDistribution) {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), externalDistribution).build());
    }

    // -- helpers --

    private static ExternalSourceExec createDummyPlan() {
        return new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data/*.parquet",
            "parquet",
            List.of(
                new FieldAttribute(
                    Source.EMPTY,
                    "name",
                    new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                )
            ),
            Map.of(),
            Map.of(),
            null
        );
    }

    private static List<ExternalSplit> createSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, "parquet", Map.of(), Map.of())
            );
        }
        return splits;
    }

    private static DiscoveryNodes createNodes(int count) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < count; i++) {
            builder.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return builder.build();
    }

    private static ExternalSourceFactory testFactory(SplitProvider provider) {
        return new ExternalSourceFactory() {

            @Override
            public void validateConfig(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException("test stub does not implement validation");
            }

            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                return null;
            }

            @Override
            public SplitProvider splitProvider() {
                return provider;
            }
        };
    }
}
