/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reproduces <a href="https://github.com/elastic/elasticsearch/issues/141318">issue #141318</a>:
 * {@code evaluateFinal} throws {@link ArrayIndexOutOfBoundsException} when the {@code selected}
 * vector contains a group id that exceeds the state's allocated capacity. The fix reorders the
 * condition so the bounds check runs before {@code hasValue()}.
 */
public class CentroidGroupingAggregatorTests extends ComputeTestCase {

    public void testPointCentroidEvaluateFinalWithOutOfBoundsSelectedGroup() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = CentroidPointAggregator.initGrouping(blockFactory.bigArrays(), CoordinateEncoder.GEO);
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2);
            var evaluationContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            CentroidPointAggregator.combineIntermediate(state, 0, 10.0, 0.0, 20.0, 0.0, 1L);
            try (Block result = CentroidPointAggregator.evaluateFinal(state, selected, evaluationContext)) {
                assertThat(result.getPositionCount(), equalTo(2));
                assertThat("in-bounds group should have a centroid result", BlockUtils.toJavaObject(result, 0), notNullValue());
                assertThat(
                    "out-of-bounds group should produce null, not ArrayIndexOutOfBoundsException",
                    BlockUtils.toJavaObject(result, 1),
                    nullValue()
                );
            }
        }
    }

    public void testShapeCentroidEvaluateFinalWithOutOfBoundsSelectedGroup() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = CentroidShapeAggregator.initGrouping(blockFactory.bigArrays(), CoordinateEncoder.GEO);
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2);
            var evaluationContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            CentroidShapeAggregator.combineIntermediate(state, 0, 10.0, 0.0, 20.0, 0.0, 1.0, DimensionalShapeType.POINT.ordinal());
            try (Block result = CentroidShapeAggregator.evaluateFinal(state, selected, evaluationContext)) {
                assertThat(result.getPositionCount(), equalTo(2));
                assertThat("in-bounds group should have a centroid result", BlockUtils.toJavaObject(result, 0), notNullValue());
                assertThat(
                    "out-of-bounds group should produce null, not ArrayIndexOutOfBoundsException",
                    BlockUtils.toJavaObject(result, 1),
                    nullValue()
                );
            }
        }
    }
}
