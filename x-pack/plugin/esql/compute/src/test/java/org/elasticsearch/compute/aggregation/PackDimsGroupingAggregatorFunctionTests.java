/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DimsPacker;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PackDimsGroupingAggregatorFunctionTests extends ComputeTestCase {

    public void testSimple() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        int prefixBlocks = between(1, 3);
        int dim0Channel = prefixBlocks;
        int dim1Channel = prefixBlocks + 1;
        int groupChannel = prefixBlocks + 2;

        Map<Integer, List<BytesRef>> expectedDim0 = new HashMap<>();
        Map<Integer, List<BytesRef>> expectedDim1 = new HashMap<>();
        List<Page> pages = new ArrayList<>();
        int numPages = between(1, 5);
        for (int i = 0; i < numPages; i++) {
            int positions = between(1, 50);
            try (
                var dim0 = blockFactory.newBytesRefBlockBuilder(positions);
                var dim1 = blockFactory.newBytesRefBlockBuilder(positions);
                var groups = blockFactory.newIntVectorFixedBuilder(positions)
            ) {
                for (int p = 0; p < positions; p++) {
                    BytesRef v0 = new BytesRef(randomAlphaOfLength(8));
                    BytesRef v1 = new BytesRef(randomAlphaOfLength(8));
                    dim0.appendBytesRef(v0);
                    dim1.appendBytesRef(v1);
                    int group = between(0, 20);
                    groups.appendInt(group);
                    expectedDim0.putIfAbsent(group, List.of(BytesRef.deepCopyOf(v0)));
                    expectedDim1.putIfAbsent(group, List.of(BytesRef.deepCopyOf(v1)));
                }
                Block[] blocks = new Block[prefixBlocks + 3];
                for (int b = 0; b < prefixBlocks; b++) {
                    blocks[b] = blockFactory.newConstantNullBlock(positions);
                }
                blocks[dim0Channel] = dim0.build();
                blocks[dim1Channel] = dim1.build();
                blocks[groupChannel] = groups.build().asBlock();
                pages.add(new Page(blocks));
            }
        }

        var aggregatorFactory = new PackDimsAggregatorFunctionSupplier().groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(dim0Channel, dim1Channel)
        );
        List<BlockHash.GroupSpec> groupSpecs = List.of(new BlockHash.GroupSpec(groupChannel, ElementType.INT));
        HashAggregationOperator operator = new HashAggregationOperator.Builder().mode(AggregatorMode.SINGLE)
            .aggregators(List.of(aggregatorFactory))
            .groups(groupSpecs)
            .maxPageSize(Integer.MAX_VALUE)
            .partialEmit(randomIntBetween(SourceOperator.MIN_TARGET_PAGE_SIZE, SourceOperator.TARGET_PAGE_SIZE / 10), 1.0)
            .build()
            .get(driverContext);

        List<Page> outputPages = new ArrayList<>();
        Driver driver = TestDriverFactory.create(
            driverContext,
            new CannedSourceOperator(pages.iterator()),
            List.of(operator),
            new PageConsumerOperator(outputPages::add)
        );
        new TestDriverRunner().run(driver);

        Map<Integer, List<BytesRef>> actualDim0 = new HashMap<>();
        Map<Integer, List<BytesRef>> actualDim1 = new HashMap<>();
        ElementType[] types = { ElementType.BYTES_REF, ElementType.BYTES_REF };
        for (Page out : outputPages) {
            IntBlock groups = out.getBlock(0);
            BytesRefBlock packed = out.getBlock(1);
            Block[] unpacked = DimsPacker.unpackMultiColumns(driverContext, packed.asVector(), types);
            try {
                BytesRefBlock dim0Block = (BytesRefBlock) unpacked[0];
                BytesRefBlock dim1Block = (BytesRefBlock) unpacked[1];
                for (int p = 0; p < out.getPositionCount(); p++) {
                    int group = groups.getInt(p);
                    actualDim0.put(group, List.of(BytesRef.deepCopyOf(dim0Block.getBytesRef(p, new BytesRef()))));
                    actualDim1.put(group, List.of(BytesRef.deepCopyOf(dim1Block.getBytesRef(p, new BytesRef()))));
                }
            } finally {
                Releasables.close(unpacked);
                out.close();
            }
        }
        assertThat(actualDim0, equalTo(expectedDim0));
        assertThat(actualDim1, equalTo(expectedDim1));
    }

    /**
     * When intermediate input arrives as an {@link OrdinalBytesRefVector}, the aggregator
     * should store ordinal indices rather than materializing bytes, and emit an
     * {@link OrdinalBytesRefBlock} so downstream operators can exploit the ordinal structure.
     */
    public void testIntermediateOrdinalInput() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        // 3 distinct packed dim values (one per TSID group)
        int dictSize = 3;
        BytesRef[] dictValues = new BytesRef[dictSize];
        for (int i = 0; i < dictSize; i++) {
            dictValues[i] = new BytesRef(randomAlphaOfLength(10));
        }
        BytesRefVector dict;
        try (var dictBuilder = blockFactory.newBytesRefVectorBuilder(dictSize)) {
            for (BytesRef v : dictValues) {
                dictBuilder.appendBytesRef(v);
            }
            dict = dictBuilder.build();
        }

        // Groups [0, 1, 2] map to ordinals [0, 1, 2]
        int positionCount = dictSize;
        OrdinalBytesRefVector ordinalVec = new OrdinalBytesRefVector(
            blockFactory.newIntArrayVector(new int[] { 0, 1, 2 }, positionCount),
            dict
        );
        Page page = new Page(ordinalVec.asBlock());
        IntVector groups;
        try (var gb = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                gb.appendInt(p);
            }
            groups = gb.build();
        }

        var agg = new PackDimsGroupingAggregatorFunction(List.of(0), driverContext);
        try (var evalCtx = new GroupingAggregatorEvaluationContext(driverContext)) {
            agg.addIntermediateInput(0, groups, page);

            IntVector allSelected;
            try (var sb = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    sb.appendInt(p);
                }
                allSelected = sb.build();
            }

            var prepared = agg.prepareEvaluateIntermediate(allSelected, evalCtx);
            Block[] output = new Block[1];
            try {
                prepared.evaluate(output, 0, allSelected);
                // Pure-ordinal path: output must be ordinal-backed — no byte copying
                assertThat(output[0], instanceOf(OrdinalBytesRefBlock.class));
                BytesRefBlock outBlock = (BytesRefBlock) output[0];
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    assertThat(outBlock.getBytesRef(p, scratch), equalTo(dictValues[p]));
                }
            } finally {
                Releasables.close(output[0], allSelected);
            }
        } finally {
            Releasables.close(agg, groups, page);
        }
    }

    /**
     * When multiple groups in the same ordinal page share the same dictionary ordinal,
     * each group should map to a single entry in the local values array — not one per group.
     * This exercises the deduplication path in {@code addOrdinalVector}.
     */
    public void testIntermediateOrdinalInputSharedOrdinal() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        // 2 distinct values in dict; 4 groups — groups 0,2 share ordinal 0, groups 1,3 share ordinal 1
        BytesRef val0 = new BytesRef(randomAlphaOfLength(8));
        BytesRef val1 = new BytesRef(randomAlphaOfLength(8));
        BytesRefVector dict;
        try (var b = blockFactory.newBytesRefVectorBuilder(2)) {
            b.appendBytesRef(val0);
            b.appendBytesRef(val1);
            dict = b.build();
        }
        // positions: group 0 → ord 0, group 1 → ord 1, group 2 → ord 0, group 3 → ord 1
        OrdinalBytesRefVector ordinalVec = new OrdinalBytesRefVector(
            blockFactory.newIntArrayVector(new int[] { 0, 1, 0, 1 }, 4),
            dict
        );
        Page page = new Page(ordinalVec.asBlock());
        IntVector groups = blockFactory.newIntArrayVector(new int[] { 0, 1, 2, 3 }, 4);

        var agg = new PackDimsGroupingAggregatorFunction(List.of(0), driverContext);
        try (var evalCtx = new GroupingAggregatorEvaluationContext(driverContext)) {
            agg.addIntermediateInput(0, groups, page);

            IntVector allSelected = blockFactory.newIntArrayVector(new int[] { 0, 1, 2, 3 }, 4);
            var prepared = agg.prepareEvaluateIntermediate(allSelected, evalCtx);
            Block[] output = new Block[1];
            try {
                prepared.evaluate(output, 0, allSelected);
                assertThat(output[0], instanceOf(OrdinalBytesRefBlock.class));
                // The output dict must have exactly 2 entries — not 4 — proving dedup worked
                OrdinalBytesRefBlock outOrdBlock = (OrdinalBytesRefBlock) output[0];
                assertThat(outOrdBlock.getDictionaryVector().getPositionCount(), equalTo(2));
                BytesRefBlock outBlock = (BytesRefBlock) output[0];
                BytesRef scratch = new BytesRef();
                assertThat(outBlock.getBytesRef(0, scratch), equalTo(val0));
                assertThat(outBlock.getBytesRef(1, scratch), equalTo(val1));
                assertThat(outBlock.getBytesRef(2, scratch), equalTo(val0));
                assertThat(outBlock.getBytesRef(3, scratch), equalTo(val1));
            } finally {
                Releasables.close(output[0], allSelected);
            }
        } finally {
            Releasables.close(agg, groups, page);
        }
    }

    /**
     * When non-ordinal intermediate input follows ordinal input, the aggregator must flush
     * the ordinal state into byte mode and continue accumulating correctly. Values from
     * both batches must be present and correct in the final output.
     */
    public void testIntermediateFallbackToByteMode() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        // --- first batch: ordinal, groups 0..2 ---
        BytesRef[] ordinalValues = { new BytesRef("alpha"), new BytesRef("beta"), new BytesRef("gamma") };
        BytesRefVector dict;
        try (var b = blockFactory.newBytesRefVectorBuilder(3)) {
            for (BytesRef v : ordinalValues)
                b.appendBytesRef(v);
            dict = b.build();
        }
        OrdinalBytesRefVector ordinalVec = new OrdinalBytesRefVector(blockFactory.newIntArrayVector(new int[] { 0, 1, 2 }, 3), dict);
        Page ordinalPage = new Page(ordinalVec.asBlock());
        IntVector ordinalGroups;
        try (var gb = blockFactory.newIntVectorFixedBuilder(3)) {
            gb.appendInt(0);
            gb.appendInt(1);
            gb.appendInt(2);
            ordinalGroups = gb.build();
        }

        // --- second batch: plain bytes, groups 3..4 ---
        BytesRef[] plainValues = { new BytesRef("delta"), new BytesRef("epsilon") };
        Page plainPage;
        try (var b = blockFactory.newBytesRefBlockBuilder(2)) {
            for (BytesRef v : plainValues)
                b.appendBytesRef(v);
            plainPage = new Page(b.build());
        }
        IntVector plainGroups;
        try (var gb = blockFactory.newIntVectorFixedBuilder(2)) {
            gb.appendInt(3);
            gb.appendInt(4);
            plainGroups = gb.build();
        }

        var agg = new PackDimsGroupingAggregatorFunction(List.of(0), driverContext);
        try (var evalCtx = new GroupingAggregatorEvaluationContext(driverContext)) {
            agg.addIntermediateInput(0, ordinalGroups, ordinalPage);
            // Non-ordinal input must trigger flush + fallback without error
            agg.addIntermediateInput(0, plainGroups, plainPage);

            int totalGroups = 5;
            IntVector allSelected;
            try (var sb = blockFactory.newIntVectorFixedBuilder(totalGroups)) {
                for (int p = 0; p < totalGroups; p++)
                    sb.appendInt(p);
                allSelected = sb.build();
            }

            var prepared = agg.prepareEvaluateFinal(allSelected, evalCtx);
            Block[] output = new Block[1];
            try {
                prepared.evaluate(output, 0, allSelected);
                BytesRefBlock outBlock = (BytesRefBlock) output[0];
                BytesRef scratch = new BytesRef();
                // Groups from the ordinal batch
                for (int p = 0; p < ordinalValues.length; p++) {
                    assertThat("group " + p, outBlock.getBytesRef(p, scratch), equalTo(ordinalValues[p]));
                }
                // Groups from the plain-bytes batch
                for (int p = 0; p < plainValues.length; p++) {
                    assertThat("group " + (p + 3), outBlock.getBytesRef(p + 3, scratch), equalTo(plainValues[p]));
                }
            } finally {
                Releasables.close(output[0], allSelected);
            }
        } finally {
            Releasables.close(agg, ordinalGroups, ordinalPage, plainGroups, plainPage);
        }
    }
}
