/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;

public class CoordinatorOnlyStrategyTests extends ESTestCase {

    public void testAlwaysReturnsLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createPlan(),
            createSplits(5),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = CoordinatorOnlyStrategy.INSTANCE.planDistribution(context);

        assertFalse(plan.distributed());
        assertTrue(plan.nodeAssignments().isEmpty());
    }

    public void testEmptySplitsReturnsLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(createPlan(), List.of(), createNodes(3), QueryPragmas.EMPTY);

        ExternalDistributionPlan plan = CoordinatorOnlyStrategy.INSTANCE.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testAlwaysReturnsLocalOnIndexSearchTopology() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .build();

        ExternalDistributionPlan plan = CoordinatorOnlyStrategy.INSTANCE.planDistribution(
            new ExternalDistributionContext(createPlan(), createSplits(8), nodes, QueryPragmas.EMPTY)
        );

        assertFalse(plan.distributed());
        assertTrue(plan.nodeAssignments().isEmpty());
    }

    public void testAlwaysReturnsLocalOnIndexOnlyTopology() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .build();

        ExternalDistributionPlan plan = CoordinatorOnlyStrategy.INSTANCE.planDistribution(
            new ExternalDistributionContext(createPlan(), createSplits(8), nodes, QueryPragmas.EMPTY)
        );

        assertFalse(plan.distributed());
    }

    private static PhysicalPlan createPlan() {
        ExternalSourceExec source = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/*.parquet",
            "parquet",
            List.of(),
            Map.of(),
            Map.of(),
            null
        );
        return new LimitExec(Source.EMPTY, source, new Literal(Source.EMPTY, 10, DataType.INTEGER), null);
    }

    private static List<ExternalSplit> createSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
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
}
