/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SinglePassPartitionedAggregatorV2Tests extends ESTestCase {

    public void testBasicLongLongCount() throws Exception {
        assumeTrue("SwissHashFactory requires jdk.incubator.vector", SwissHashFactory.getInstance() != null);

        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).build();
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);

        int numPages = 3;
        int rowsPerPage = 10; // 5 key0 values × 2 key1 values

        Map<Long, Map<Long, Long>> expected = new HashMap<>();
        for (int k0 = 0; k0 < 5; k0++) {
            for (int k1 = 0; k1 < 2; k1++) {
                expected.computeIfAbsent((long) k0, x -> new HashMap<>()).put((long) k1, (long) numPages);
            }
        }

        AggregatorFunctionSupplier supplier = CountAggregatorFunction.supplier();
        var executor = Executors.newFixedThreadPool(4);
        try {
            var op = new SinglePassPartitionedAggregatorV2(0, 1, false, List.of(supplier), List.of(List.of()), 2, executor, driverContext);

            for (int p = 0; p < numPages; p++) {
                long[] key0s = new long[rowsPerPage];
                long[] key1s = new long[rowsPerPage];
                int idx = 0;
                for (int k0 = 0; k0 < 5; k0++) {
                    for (int k1 = 0; k1 < 2; k1++) {
                        key0s[idx] = k0;
                        key1s[idx] = k1;
                        idx++;
                    }
                }
                Page page = new Page(
                    blockFactory.newLongArrayVector(key0s, rowsPerPage).asBlock(),
                    blockFactory.newLongArrayVector(key1s, rowsPerPage).asBlock()
                );
                op.addInput(page);
            }
            op.finish();

            List<Page> outputPages = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (op.isFinished() == false) {
                Page out = op.getOutput();
                if (out != null) {
                    outputPages.add(out);
                }
                if (System.currentTimeMillis() > deadline) {
                    fail("timed out waiting for SPPA v2 to finish");
                }
                Thread.sleep(10);
            }
            Page out;
            while ((out = op.getOutput()) != null) {
                outputPages.add(out);
            }

            Map<Long, Map<Long, Long>> actual = new HashMap<>();
            for (Page page : outputPages) {
                LongVector k0Vec = ((LongBlock) page.getBlock(0)).asVector();
                LongVector k1Vec = ((LongBlock) page.getBlock(1)).asVector();
                LongVector countVec = ((LongBlock) page.getBlock(2)).asVector();
                assertNotNull("key0 block must be dense", k0Vec);
                assertNotNull("key1 block must be dense", k1Vec);
                assertNotNull("count block must be dense", countVec);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    actual.computeIfAbsent(k0Vec.getLong(i), x -> new HashMap<>()).put(k1Vec.getLong(i), countVec.getLong(i));
                }
                page.releaseBlocks();
            }

            assertEquals(expected, actual);
            op.close();
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
