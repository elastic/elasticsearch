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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
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
}
