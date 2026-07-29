/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link PartitionedHashAggregationOperator#canPartition},
 * {@link DriverContext#withBlockFactory}.
 */
public class PartitionedHashAggregationOperatorCanPartitionAndSortTests extends ESTestCase {

    // ---- canPartition ----

    public void testCanPartitionSingleColumnSupportedTypes() {
        for (ElementType type : List.of(
            ElementType.LONG,
            ElementType.INT,
            ElementType.DOUBLE,
            ElementType.BYTES_REF,
            ElementType.BOOLEAN
        )) {
            assertTrue(
                "single-column " + type + " should be partitionable",
                PartitionedHashAggregationOperator.canPartition(List.of(new BlockHash.GroupSpec(0, type)))
            );
        }
    }

    public void testCanPartitionMultiColumnAllSupported() {
        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG),
            new BlockHash.GroupSpec(1, ElementType.INT),
            new BlockHash.GroupSpec(2, ElementType.DOUBLE),
            new BlockHash.GroupSpec(3, ElementType.BYTES_REF),
            new BlockHash.GroupSpec(4, ElementType.BOOLEAN)
        );
        assertTrue(PartitionedHashAggregationOperator.canPartition(specs));
    }

    public void testCanPartitionMultiColumnUnsupportedType() {
        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG),
            new BlockHash.GroupSpec(1, ElementType.NULL)
        );
        assertFalse(PartitionedHashAggregationOperator.canPartition(specs));
    }

    public void testCanPartitionSingleColumnUnsupportedType() {
        // size <= 1 short-circuits to true regardless of type
        assertTrue(PartitionedHashAggregationOperator.canPartition(List.of(new BlockHash.GroupSpec(0, ElementType.NULL))));
    }

    public void testCanPartitionEmptyListReturnTrue() {
        assertTrue(PartitionedHashAggregationOperator.canPartition(List.of()));
    }

    public void testCanPartitionReturnsFalseForTopNSingleColumn() {
        // TopN groups are bounded by limit — never crosses the conversion threshold.
        BlockHash.TopNDef topN = new BlockHash.TopNDef(List.of(new BlockHash.SortKey(0, true, false)), 10);
        BlockHash.GroupSpec spec = new BlockHash.GroupSpec(0, ElementType.LONG, null, topN);
        assertFalse(PartitionedHashAggregationOperator.canPartition(List.of(spec)));
    }

    public void testCanPartitionReturnsFalseForTopNMultiColumn() {
        BlockHash.TopNDef topN = new BlockHash.TopNDef(List.of(new BlockHash.SortKey(0, true, false)), 10);
        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG, null, topN),
            new BlockHash.GroupSpec(1, ElementType.INT)
        );
        assertFalse(PartitionedHashAggregationOperator.canPartition(specs));
    }

    public void testCanPartitionReturnsFalseForCategorizeSingleColumn() {
        // Categorize uses semantic equality incompatible with key-space partitioning.
        BlockHash.CategorizeDef categorize = new BlockHash.CategorizeDef("standard", BlockHash.CategorizeDef.OutputFormat.REGEX, 70);
        BlockHash.GroupSpec spec = new BlockHash.GroupSpec(0, ElementType.BYTES_REF, categorize);
        assertFalse(PartitionedHashAggregationOperator.canPartition(List.of(spec)));
    }

    public void testCanPartitionReturnsFalseForCategorizeMultiColumn() {
        BlockHash.CategorizeDef categorize = new BlockHash.CategorizeDef("standard", BlockHash.CategorizeDef.OutputFormat.REGEX, 70);
        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.BYTES_REF, categorize),
            new BlockHash.GroupSpec(1, ElementType.LONG)
        );
        assertFalse(PartitionedHashAggregationOperator.canPartition(specs));
    }

    // ---- DriverContext.withBlockFactory ----

    public void testWithBlockFactorySharedBigArrays() {
        BlockFactory parentFactory = new BlockFactory(new NoopCircuitBreaker("parent"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext parent = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, parentFactory, null);
        BlockFactory childFactory = new BlockFactory(new NoopCircuitBreaker("child"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext child = parent.withBlockFactory(childFactory);
        assertThat(child.bigArrays(), sameInstance(parent.bigArrays()));
    }

    public void testWithBlockFactoryUsesNewBlockFactory() {
        BlockFactory parentFactory = new BlockFactory(new NoopCircuitBreaker("parent"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext parent = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, parentFactory, null);
        BlockFactory childFactory = new BlockFactory(new NoopCircuitBreaker("child"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext child = parent.withBlockFactory(childFactory);
        assertThat(child.blockFactory(), sameInstance(childFactory));
        assertThat(child.blockFactory(), not(sameInstance(parent.blockFactory())));
    }

    public void testWithBlockFactorySharedWarningsMode() {
        BlockFactory parentFactory = new BlockFactory(new NoopCircuitBreaker("parent"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext parent = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, parentFactory, null);
        BlockFactory childFactory = new BlockFactory(new NoopCircuitBreaker("child"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext child = parent.withBlockFactory(childFactory);
        assertThat(child.warningsMode(), equalTo(parent.warningsMode()));
    }
}
