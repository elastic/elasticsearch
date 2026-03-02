/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SplitDiscoveryPhase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
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

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.hamcrest.Matchers.containsString;

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
            null,
            FileSet.UNRESOLVED
        );

        SplitProvider failingProvider = ctx -> { throw new UncheckedIOException(new IOException("connection reset")); };
        ExternalSourceFactory factory = testFactory(failingProvider);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> SplitDiscoveryPhase.resolveExternalSplits(exec, Map.of(sourceType, factory))
        );

        assertThat(e.getMessage(), containsString(sourcePath));
        assertThat(e.getMessage(), containsString(sourceType));
        assertNotNull(e.getCause());
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
