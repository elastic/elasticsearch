/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.Nullable;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

public class TsInfoOperatorTests extends OperatorTestCase {

    private static final MetricsInfoOperator.MetricFieldLookup SIMPLE_LOOKUP = (indexName, fieldName) -> switch (fieldName) {
        case "cpu_usage" -> new MetricFieldInfo("cpu_usage", indexName, "double", "gauge", "percent");
        case "disk_io" -> new MetricFieldInfo("disk_io", indexName, "long", "counter", "bytes");
        default -> null;
    };

    private static final int METADATA_CHANNEL = 0;
    private static final int INDEX_CHANNEL = 1;
    private static final int[] FINAL_CHANNELS = { 0, 1, 2, 3, 4, 5, 6 };

    @Override
    protected TsInfoOperator.Factory simple(SimpleOptions options) {
        return new TsInfoOperator.Factory(SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SourceOperator() {
            private int position = 0;
            private static final int PAGE_SIZE = 100;

            @Override
            public void finish() {
                position = size;
            }

            @Override
            public boolean isFinished() {
                return position >= size;
            }

            @Override
            public Page getOutput() {
                if (isFinished()) {
                    return null;
                }
                int remaining = size - position;
                int pageSize = Math.min(PAGE_SIZE, remaining);
                try (
                    BytesRefBlock.Builder metadataBuilder = blockFactory.newBytesRefBlockBuilder(pageSize);
                    BytesRefBlock.Builder indexBuilder = blockFactory.newBytesRefBlockBuilder(pageSize)
                ) {
                    for (int i = 0; i < pageSize; i++) {
                        int idx = position + i;
                        String json = "{\"cpu_usage\": " + (idx * 0.1) + ", \"host\": \"server" + (idx % 3) + "\"}";
                        metadataBuilder.appendBytesRef(new BytesRef(json));
                        indexBuilder.appendBytesRef(new BytesRef("index-" + (idx % 2)));
                    }
                    position += pageSize;
                    return new Page(metadataBuilder.build(), indexBuilder.build());
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("TsInfoOperator[mode=INITIAL, metadataSourceChannel=0, indexChannel=1]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("TsInfoOperator[mode=INITIAL]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
        // cpu_usage is the only metric; each unique (metric, index, dimensions) combo is a row.
        // With 3 hosts and 2 indices → up to 6 rows (each host value creates a different dimensions JSON)
        assertTrue("Expected at least 1 row, got " + totalRows, totalRows >= 1);
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(TsInfoOperator.NUM_BLOCKS));
        }
    }

    @Override
    protected void assertStatus(@Nullable Map<String, Object> map, List<Page> input, List<Page> output) {
        assertNull(map);
    }

    private Operator createInitialOperator() {
        return new TsInfoOperator.Factory(SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL).get(driverContext());
    }

    private Operator createFinalOperator(int[] channels) {
        return new TsInfoOperator.FinalFactory(channels).get(driverContext());
    }

    public void testInitialModeSingleTsidSingleMetric() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.85, \"host\": \"server1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertThat(output.getBlockCount(), equalTo(TsInfoOperator.NUM_BLOCKS));

            assertColumnValue(output, 0, 0, "cpu_usage");       // metric_name
            assertColumnValue(output, 1, 0, "my-index");         // data_stream
            assertColumnValue(output, 2, 0, "percent");           // unit
            assertColumnValue(output, 3, 0, "gauge");             // metric_type
            assertColumnValue(output, 4, 0, "double");            // field_type
            assertColumnValue(output, 5, 0, "host");              // dimension_fields

            // dimensions should be a JSON string containing host dimension
            String dimensions = readSingleValue(output, 6, 0);
            assertNotNull(dimensions);
            assertTrue("Dimensions should contain host key: " + dimensions, dimensions.contains("\"host\""));
            assertTrue("Dimensions should contain server1 value: " + dimensions, dimensions.contains("server1"));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testInitialModeMultipleTsidsProducesMultipleRows() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            // Two tsids with different dimension values → two rows for the same metric
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "my-index");
            op.addInput(input1);

            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"server2\"}", "my-index");
            op.addInput(input2);

            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Different dimension values → different dimensions JSON → 2 rows
            assertThat(output.getPositionCount(), equalTo(2));

            // Both rows should be cpu_usage
            Set<String> metricNames = collectColumnValues(output, 0);
            assertThat(metricNames, equalTo(Set.of("cpu_usage")));

            // Collect all dimensions JSON values
            Set<String> allDimensions = collectColumnValues(output, 6);
            assertThat(allDimensions.size(), equalTo(2));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testInitialModeDimensionsJsonIsSorted() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            // Metadata with multiple dimension keys: "host" and "az" — keys should be sorted in JSON
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\", \"az\": \"us-east-1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            String dimensions = readSingleValue(output, 6, 0);
            assertNotNull(dimensions);
            // "az" should come before "host" in sorted order
            int azIdx = dimensions.indexOf("\"az\"");
            int hostIdx = dimensions.indexOf("\"host\"");
            assertTrue("Keys should be sorted: az before host in " + dimensions, azIdx < hostIdx);

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testFinalModeMergesRowsBySignature() {
        BlockFactory blockFactory = driverContext().blockFactory();
        int[] channels = { 0, 1, 2, 3, 4, 5, 6 };
        Operator op = createFinalOperator(channels);
        try {
            // Two identical rows from two data nodes → should merge into 1
            String dimJson = "{\"host\": \"server1\"}";
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                dimJson
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                dimJson
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testFinalModeUnionsDataStreams() {
        BlockFactory blockFactory = driverContext().blockFactory();
        int[] channels = { 0, 1, 2, 3, 4, 5, 6 };
        Operator op = createFinalOperator(channels);
        try {
            String dimJson = "{\"host\": \"server1\"}";
            // Data node 1 saw index-a; data node 2 saw index-b — same metric + same dimensions
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                dimJson
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-b"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                dimJson
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            // data_stream should be union of both
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("index-a", "index-b")));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testEmptyInputProducesEmptyPage() {
        Operator op = createInitialOperator();
        try {
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(0));
            assertThat(output.getBlockCount(), equalTo(TsInfoOperator.NUM_BLOCKS));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testDimensionsColumnContainsJsonObject() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\", \"region\": \"eu\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            String dimensions = readSingleValue(output, 6, 0);
            assertNotNull(dimensions);
            assertTrue("Should start with {: " + dimensions, dimensions.startsWith("{"));
            assertTrue("Should end with }: " + dimensions, dimensions.endsWith("}"));
            assertTrue("Should contain host: " + dimensions, dimensions.contains("\"host\": \"server1\""));
            assertTrue("Should contain region: " + dimensions, dimensions.contains("\"region\": \"eu\""));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testOutputHasSevenColumns() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getBlockCount(), equalTo(7));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testFinalModeDifferentDimensionsRemainSeparateRows() {
        BlockFactory blockFactory = driverContext().blockFactory();
        int[] channels = { 0, 1, 2, 3, 4, 5, 6 };
        Operator op = createFinalOperator(channels);
        try {
            // Same metric but different dimensions → separate rows
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                "{\"host\": \"server1\"}"
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                "{\"host\": \"server2\"}"
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(2));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testFinalModeEmptyInputProducesEmptyOutput() {
        int[] channels = { 0, 1, 2, 3, 4, 5, 6 };
        Operator op = createFinalOperator(channels);
        try {
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(0));
            assertThat(output.getBlockCount(), equalTo(TsInfoOperator.NUM_BLOCKS));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    private static Page buildPage(BlockFactory blockFactory, String metadataJson, String indexName) {
        try (
            BytesRefBlock.Builder metadataBuilder = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder indexBuilder = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            metadataBuilder.appendBytesRef(new BytesRef(metadataJson));
            indexBuilder.appendBytesRef(new BytesRef(indexName));
            return new Page(metadataBuilder.build(), indexBuilder.build());
        }
    }

    private static Page buildFinalPage(
        BlockFactory blockFactory,
        String metricName,
        Set<String> dataStreams,
        Set<String> units,
        Set<String> metricTypes,
        Set<String> fieldTypes,
        Set<String> dimensionFields,
        String dimensionsJson
    ) {
        try (
            BytesRefBlock.Builder nameB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder dsB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder unitB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder mtB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder ftB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder dfB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder dimB = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            nameB.appendBytesRef(new BytesRef(metricName));
            appendSet(dsB, dataStreams);
            appendSet(unitB, units);
            appendSet(mtB, metricTypes);
            appendSet(ftB, fieldTypes);
            appendSet(dfB, dimensionFields);
            dimB.appendBytesRef(new BytesRef(dimensionsJson));
            return new Page(nameB.build(), dsB.build(), unitB.build(), mtB.build(), ftB.build(), dfB.build(), dimB.build());
        }
    }

    private static void appendSet(BytesRefBlock.Builder builder, Set<String> values) {
        if (values == null || values.isEmpty()) {
            builder.appendNull();
        } else if (values.size() == 1) {
            builder.appendBytesRef(new BytesRef(values.iterator().next()));
        } else {
            builder.beginPositionEntry();
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            builder.endPositionEntry();
        }
    }

    private static void assertColumnValue(Page page, int blockIndex, int position, String expected) {
        BytesRefBlock block = page.getBlock(blockIndex);
        if (block.getValueCount(position) == 1) {
            assertThat(block.getBytesRef(block.getFirstValueIndex(position), new BytesRef()).utf8ToString(), equalTo(expected));
        } else {
            Set<String> values = new HashSet<>();
            int start = block.getFirstValueIndex(position);
            int count = block.getValueCount(position);
            for (int i = start; i < start + count; i++) {
                values.add(block.getBytesRef(i, new BytesRef()).utf8ToString());
            }
            assertTrue("Expected [" + expected + "] in " + values, values.contains(expected));
        }
    }

    private static String readSingleValue(Page page, int blockIndex, int position) {
        BytesRefBlock block = page.getBlock(blockIndex);
        if (block.isNull(position)) {
            return null;
        }
        return block.getBytesRef(block.getFirstValueIndex(position), new BytesRef()).utf8ToString();
    }

    private static Set<String> collectColumnValues(Page page, int blockIndex) {
        BytesRefBlock block = page.getBlock(blockIndex);
        Set<String> values = new TreeSet<>();
        for (int p = 0; p < page.getPositionCount(); p++) {
            if (block.isNull(p) == false) {
                int start = block.getFirstValueIndex(p);
                int count = block.getValueCount(p);
                for (int i = start; i < start + count; i++) {
                    values.add(block.getBytesRef(i, new BytesRef()).utf8ToString());
                }
            }
        }
        return values;
    }

    private static Set<String> collectMultiValues(Page page, int blockIndex, int position) {
        BytesRefBlock block = page.getBlock(blockIndex);
        Set<String> values = new HashSet<>();
        if (block.isNull(position) == false) {
            int start = block.getFirstValueIndex(position);
            int count = block.getValueCount(position);
            for (int i = start; i < start + count; i++) {
                values.add(block.getBytesRef(i, new BytesRef()).utf8ToString());
            }
        }
        return values;
    }

    public void testInitialModeTracksMemoryOnNewEntries() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();
        try (TsInfoOperator op = (TsInfoOperator) new TsInfoOperator.Factory(SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL).get(ctx)) {
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a");
            op.addInput(input1);
            long usedAfterOne = blockFactory.breaker().getUsed();
            assertThat(usedAfterOne - usedBefore, equalTo(TsInfoOperator.SHALLOW_SIZE));

            // Second distinct (metric, data stream, dimensions) adds another entry
            Page input2 = buildPage(blockFactory, "{\"disk_io\": 1024, \"host\": \"h1\"}", "index-a");
            op.addInput(input2);
            long usedAfterTwo = blockFactory.breaker().getUsed();
            assertThat(usedAfterTwo - usedBefore, equalTo(2 * TsInfoOperator.SHALLOW_SIZE));

            // Same (cpu_usage, index-a, {"host":"h1"}) again → no new entry
            Page input3 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"h1\"}", "index-a");
            op.addInput(input3);
            long usedAfterDuplicate = blockFactory.breaker().getUsed();
            assertThat(usedAfterDuplicate - usedBefore, equalTo(2 * TsInfoOperator.SHALLOW_SIZE));

            op.finish();
            Page output = op.getOutput();
            assertNotNull(output);
            output.releaseBlocks();
        }
    }

    public void testFinalModeTracksMemoryOnNewEntries() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();
        try (TsInfoOperator op = (TsInfoOperator) new TsInfoOperator.FinalFactory(FINAL_CHANNELS).get(ctx)) {
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                "{}"
            );
            op.addInput(page1);
            long usedAfterOne = blockFactory.breaker().getUsed();
            assertThat(usedAfterOne - usedBefore, equalTo(TsInfoOperator.SHALLOW_SIZE));

            // Different (metricName, dimensionsJson) → new entry
            Page page2 = buildFinalPage(
                blockFactory,
                "disk_io",
                Set.of("ds-a"),
                Set.of("bytes"),
                Set.of("counter"),
                Set.of("long"),
                Set.of("host"),
                "{}"
            );
            op.addInput(page2);
            long usedAfterTwo = blockFactory.breaker().getUsed();
            assertThat(usedAfterTwo - usedBefore, equalTo(2 * TsInfoOperator.SHALLOW_SIZE));

            // Same signature (cpu_usage, {}, percent, gauge, double) → no new entry
            Page page3 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host", "region"),
                "{}"
            );
            op.addInput(page3);
            long usedAfterDuplicate = blockFactory.breaker().getUsed();
            assertThat(usedAfterDuplicate - usedBefore, equalTo(2 * TsInfoOperator.SHALLOW_SIZE));

            op.finish();
            Page output = op.getOutput();
            assertNotNull(output);
            output.releaseBlocks();
        }
    }

    public void testCloseReleasesTrackedMemory() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();

        TsInfoOperator op = (TsInfoOperator) new TsInfoOperator.Factory(SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL).get(ctx);
        Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a");
        op.addInput(input);
        assertThat(blockFactory.breaker().getUsed() - usedBefore, equalTo(TsInfoOperator.SHALLOW_SIZE));

        op.finish();
        Page output = op.getOutput();
        assertNotNull(output);
        output.releaseBlocks();

        op.close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(usedBefore));
    }
}
