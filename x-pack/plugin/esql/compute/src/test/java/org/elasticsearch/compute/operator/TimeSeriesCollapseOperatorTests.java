/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TimeSeriesCollapseOperatorTests extends ComputeTestCase {

    public void testCollapseGroupsAndOrdersByStep() {
        List<Page> results = run(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)),
            2,
            1,
            10,
            30,
            10,
            10,
            List.of(
                List.of("h1", 30L, 3.0),
                List.of("h2", 20L, 11.0),
                List.of("h1", 10L, 1.0),
                Arrays.asList("h1", 20L, null),
                List.of("h2", 10L, 10.0),
                List.of("h2", 30L, 12.0)
            )
        );

        try {
            assertThat(results.size(), equalTo(1));
            Page page = results.get(0);
            assertThat(page.getPositionCount(), equalTo(2));
            assertThat(page.getBlockCount(), equalTo(3));

            assertRow(page, 0, "h1", List.of(10L, 30L), List.of(1.0, 3.0));
            assertRow(page, 1, "h2", List.of(10L, 20L, 30L), List.of(10.0, 11.0, 12.0));
        } finally {
            Releasables.close(results);
        }
    }

    public void testGlobalCollapseWithoutDimensions() {
        List<Page> results = run(List.of(), 1, 0, 100, 120, 10, 10, List.of(List.of(120L, 3.0), List.of(100L, 1.0)));

        try {
            assertThat(results.size(), equalTo(1));
            Page page = results.get(0);
            assertThat(page.getPositionCount(), equalTo(1));
            assertThat(page.getBlockCount(), equalTo(2));
            assertLongs(page.getBlock(0), 0, List.of(100L, 120L));
            assertDoubles(page.getBlock(1), 0, List.of(1.0, 3.0));
        } finally {
            Releasables.close(results);
        }
    }

    public void testPreservesInputChannelLayout() {
        List<Page> results = run(
            List.of(new BlockHash.GroupSpec(2, ElementType.BYTES_REF)),
            0,
            1,
            10,
            20,
            10,
            10,
            List.of(List.of(2.0, 20L, "h1"), List.of(1.0, 10L, "h1"))
        );

        try {
            assertThat(results.size(), equalTo(1));
            Page page = results.get(0);
            assertThat(page.getPositionCount(), equalTo(1));
            assertThat(page.getBlockCount(), equalTo(3));
            assertDoubles(page.getBlock(0), 0, List.of(1.0, 2.0));
            assertLongs(page.getBlock(1), 0, List.of(10L, 20L));
            assertKey(page.getBlock(2), 0, "h1");
        } finally {
            Releasables.close(results);
        }
    }

    public void testGroupWithOnlyNullValuesEmitsNullCollapseColumns() {
        List<Page> results = run(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)),
            2,
            1,
            10,
            20,
            10,
            10,
            List.of(Arrays.asList("h1", 10L, null), Arrays.asList("h1", 20L, null))
        );

        try {
            assertThat(results.size(), equalTo(1));
            Page page = results.get(0);
            assertThat(page.getPositionCount(), equalTo(1));
            assertKey(page.getBlock(0), 0, "h1");
            assertTrue(page.getBlock(1).isNull(0));
            assertTrue(page.getBlock(2).isNull(0));
        } finally {
            Releasables.close(results);
        }
    }

    public void testOutputHonorsMaxPageSize() {
        List<Page> results = run(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)),
            2,
            1,
            10,
            10,
            10,
            1,
            List.of(List.of("h1", 10L, 1.0), List.of("h2", 10L, 2.0))
        );

        try {
            assertThat(results.size(), equalTo(2));
            assertThat(results.get(0).getPositionCount(), equalTo(1));
            assertThat(results.get(1).getPositionCount(), equalTo(1));
            assertRow(results.get(0), 0, "h1", List.of(10L), List.of(1.0));
            assertRow(results.get(1), 0, "h2", List.of(10L), List.of(2.0));
        } finally {
            Releasables.close(results);
        }
    }

    public void testRejectsOutOfRangeStep() {
        try (
            Operator operator = new TimeSeriesCollapseOperator.Factory(
                List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF)),
                2,
                1,
                10,
                20,
                10
            ).get(driverContext())
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> operator.addInput(page(List.of(List.of("h1", 30L, 1.0))))
            );
            assertThat(e.getMessage(), equalTo("time series collapse timestamp [30] is outside range [10, 20]"));
        }
    }

    private List<Page> run(
        List<BlockHash.GroupSpec> groups,
        int valueChannel,
        int stepChannel,
        long start,
        long end,
        long step,
        int maxPageSize,
        List<List<Object>> rows
    ) {
        List<Page> results = new ArrayList<>();
        try (
            Operator operator = new TimeSeriesCollapseOperator.Factory(
                groups,
                valueChannel,
                stepChannel,
                start,
                end,
                step,
                maxPageSize,
                10_000
            ).get(driverContext())
        ) {
            operator.addInput(page(rows));
            operator.finish();
            Page output;
            while ((output = operator.getOutput()) != null) {
                results.add(output);
            }
        }
        return results;
    }

    private Page page(List<List<Object>> rows) {
        return new Page(BlockUtils.fromList(blockFactory(), rows));
    }

    private DriverContext driverContext() {
        return new DriverContext(blockFactory().bigArrays(), blockFactory(), null);
    }

    private void assertRow(Page page, int position, String key, List<Long> expectedSteps, List<Double> expectedValues) {
        assertKey(page.getBlock(0), position, key);
        assertLongs(page.getBlock(1), position, expectedSteps);
        assertDoubles(page.getBlock(2), position, expectedValues);
    }

    private void assertKey(Block block, int position, String expected) {
        assertThat(block, instanceOf(BytesRefBlock.class));
        BytesRefBlock keys = (BytesRefBlock) block;
        assertFalse(keys.isNull(position));
        assertThat(keys.getBytesRef(keys.getFirstValueIndex(position), new BytesRef()).utf8ToString(), equalTo(expected));
    }

    private void assertLongs(Block block, int position, List<Long> expected) {
        assertThat(block, instanceOf(LongBlock.class));
        LongBlock longs = (LongBlock) block;
        assertFalse(longs.isNull(position));
        assertThat(longs.getValueCount(position), equalTo(expected.size()));
        int first = longs.getFirstValueIndex(position);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(longs.getLong(first + i), equalTo(expected.get(i)));
        }
    }

    private void assertDoubles(Block block, int position, List<Double> expected) {
        assertThat(block, instanceOf(DoubleBlock.class));
        DoubleBlock doubles = (DoubleBlock) block;
        assertFalse(doubles.isNull(position));
        assertThat(doubles.getValueCount(position), equalTo(expected.size()));
        int first = doubles.getFirstValueIndex(position);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(doubles.getDouble(first + i), equalTo(expected.get(i)));
        }
    }
}
