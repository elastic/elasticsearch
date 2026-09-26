/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.List;

/** The per-storage-series carrier must preserve typed values across exchange and reordered output pages. */
public class DimensionValuesPackDimGroupingAggregatorFunctionTests extends ComputeTestCase {
    public void testIntermediateStateAndReorderedSelection() {
        var factory = blockFactory();
        var context = new DriverContext(factory.bigArrays(), factory, null);
        try (
            var inputBuilder = factory.newPackDimBlockBuilder(4);
            var first = new DimensionValuesPackDimGroupingAggregatorFunction(List.of(0), context);
            var second = new DimensionValuesPackDimGroupingAggregatorFunction(List.of(0), context);
            var groups = vector(0, 0, 1, 2);
            var selected = vector(0, 1, 2);
            var reordered = vector(2, 0, 1, 4);
            var evaluation = new GroupingAggregatorEvaluationContext(context)
        ) {
            for (int i = 0; i < 2; i++)
                inputBuilder.append(new BytesRef[] { new BytesRef("label") }, new BytesRef[] { new BytesRef("\"same\"") });
            inputBuilder.append(new BytesRef[0], new BytesRef[0]);
            inputBuilder.appendNull();
            try (var input = inputBuilder.build(); var add = first.prepareProcessRawInputPage(new SeenGroupIds.Empty(), new Page(input))) {
                add.add(0, groups);
            }
            Block[] intermediate = new Block[1];
            try (var prepared = first.prepareEvaluateIntermediate(selected, evaluation)) {
                prepared.evaluate(intermediate, 0, selected);
                second.addIntermediateInput(0, selected, new Page(intermediate));
            } finally {
                Releasables.close(intermediate);
            }
            Block[] result = new Block[1];
            try (var prepared = second.prepareEvaluateFinal(selected, evaluation)) {
                prepared.evaluate(result, 0, reordered);
                PackDimBlock packed = (PackDimBlock) result[0];
                assertTrue(packed.isNull(0));
                assertEquals(
                    new BytesRef("\"same\""),
                    packed.getPackDim(packed.getFirstValueIndex(1), new PackDimValue()).get(new BytesRef("label"), new BytesRef())
                );
                assertEquals(0, packed.getPackDim(packed.getFirstValueIndex(2), new PackDimValue()).size());
                assertFalse(packed.isNull(2));
                assertTrue(packed.isNull(3));
            } finally {
                Releasables.close(result);
            }
        }
    }

    private IntVector vector(int... values) {
        try (var builder = blockFactory().newIntVectorBuilder(values.length)) {
            for (int value : values)
                builder.appendInt(value);
            return builder.build();
        }
    }
}
