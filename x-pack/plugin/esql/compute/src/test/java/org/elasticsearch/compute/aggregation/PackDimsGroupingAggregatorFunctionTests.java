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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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

    private BytesRefVector randomInputVector(BytesRef[] dictValues, int size) {
        if (randomBoolean()) {
            try (var builder = blockFactory().newBytesRefVectorBuilder(size)) {
                for (int i = 0; i < size; i++) {
                    builder.appendBytesRef(randomFrom(dictValues));
                }
                return builder.build();
            }
        }
        int[] mappedOrds = new int[dictValues.length];
        Arrays.fill(mappedOrds, -1);
        int nextOrd = 0;
        try (var dictBuilder = blockFactory().newBytesRefVectorBuilder(size); var ordsBuilder = blockFactory().newIntVectorBuilder(size);) {
            int ord = randomIntBetween(0, dictValues.length - 1);
            for (int p = 0; p < size; p++) {
                int mappedOrd = mappedOrds[ord];
                if (mappedOrd == -1) {
                    mappedOrd = nextOrd++;
                    dictBuilder.appendBytesRef(dictValues[ord]);
                }
                ordsBuilder.appendInt(mappedOrd);
            }
            return new OrdinalBytesRefVector(ordsBuilder.build(), dictBuilder.build());
        }
    }

    private BytesRefBlock randomInputBlock(BytesRef[] dictValues, int size) {
        try (var builder = blockFactory().newBytesRefBlockBuilder(size)) {
            int valueCount = between(0, 2);
            if (valueCount == 1) {
                builder.appendBytesRef(randomFrom(dictValues));
            } else if (valueCount == 0) {
                builder.appendNull();
            } else {
                builder.beginPositionEntry();
                for (int i = 0; i < valueCount; i++) {
                    builder.appendBytesRef(randomFrom(dictValues));
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }

    public void testIntermediateInput() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        int dictSize = between(1, 20);
        BytesRef[] dictValues = new BytesRef[dictSize];
        for (int i = 0; i < dictSize; i++) {
            dictValues[i] = new BytesRef("v" + i);
        }
        int numPages = between(1, 5);
        var packedAggs = new PackDimsGroupingAggregatorFunction(List.of(0), driverContext);
        var dimAggs = new DimensionValuesByteRefGroupingAggregatorFunction(List.of(0), driverContext);
        int maxGroupId = -1;
        boolean fallback = false;
        for (int i = 0; i < numPages; i++) {
            final Page page;
            if (frequently()) {
                page = new Page(randomInputVector(dictValues, between(1, 100)).asBlock());
            } else {
                BytesRefBlock bytesBlock = randomInputBlock(dictValues, between(1, 100));
                fallback |= bytesBlock.asVector() == null;
                page = new Page(bytesBlock);
            }
            try (
                var addInput1 = dimAggs.prepareProcessIntermediateInputPage(null, page);
                var addInput2 = packedAggs.prepareProcessIntermediateInputPage(null, page)
            ) {
                int positionOffset = 0;
                while (positionOffset < page.getPositionCount()) {
                    int positionCount = between(1, page.getPositionCount() - positionOffset);
                    try (var groupsBuilder = blockFactory.newIntVectorBuilder(positionCount)) {
                        for (int p = 0; p < positionCount; p++) {
                            if (maxGroupId > 0 && randomBoolean()) {
                                groupsBuilder.appendInt(between(0, maxGroupId));
                            } else {
                                maxGroupId++;
                                groupsBuilder.appendInt(maxGroupId);
                            }
                        }
                        try (var groupIds = groupsBuilder.build()) {
                            addInput1.add(positionOffset, groupIds);
                            addInput2.add(positionOffset, groupIds);
                        }
                    }
                    positionOffset += positionCount;
                }
            }
            page.close();
        }

        try (var evalCtx = new GroupingAggregatorEvaluationContext(driverContext)) {
            IntVector allSelected;
            try (var sb = blockFactory.newIntVectorFixedBuilder(maxGroupId + 1)) {
                for (int g = 0; g <= maxGroupId; g++) {
                    sb.appendInt(g);
                }
                allSelected = sb.build();
            }
            try (
                allSelected;
                var eval1 = packedAggs.prepareEvaluateIntermediate(allSelected, evalCtx);
                var eval2 = dimAggs.prepareEvaluateIntermediate(allSelected, evalCtx);
            ) {
                int offset = 0;
                while (offset <= maxGroupId) {
                    int end = between(offset + 1, maxGroupId + 1);
                    Block[] out1 = new Block[1];
                    Block[] out2 = new Block[1];
                    try (var selected = blockFactory.newIntRangeVector(offset, end)) {
                        eval1.evaluate(out1, 0, selected);
                        eval2.evaluate(out2, 0, selected);
                        assertThat(out1[0], equalTo(out2[0]));
                        if (fallback == false) {
                            assertNotNull(((BytesRefBlock) out1[0]).asOrdinals());
                        }
                    } finally {
                        Releasables.close(out1);
                        Releasables.close(out2);
                    }
                    offset = end;
                }
            }
        } finally {
            Releasables.close(packedAggs, dimAggs);
        }
    }
}
