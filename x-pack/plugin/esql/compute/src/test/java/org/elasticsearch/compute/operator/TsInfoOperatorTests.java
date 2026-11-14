/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.compute.operator.MetricsInfoOperatorTests.DATA_STREAMS_BY_INDEX;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class TsInfoOperatorTests extends OperatorTestCase {

    private static final MetricsInfoOperator.MetricFieldLookup SIMPLE_LOOKUP = (indexName, fieldName) -> switch (fieldName) {
        case "cpu_usage" -> new MetricFieldInfo("cpu_usage", indexName, "double", "gauge", "percent");
        case "disk_io" -> new MetricFieldInfo("disk_io", indexName, "long", "counter", "bytes");
        default -> null;
    };

    private static final int METADATA_CHANNEL = 0;
    private static final int INDEX_CHANNEL = 1;
    private static final int[] FINAL_CHANNELS = { 0, 1, 2, 3, 4, 5, 6 };
    /** Any non-empty input reaches this threshold, so every such page is emitted early. */
    private static final ByteSizeValue EMIT_AFTER_EVERY_PAGE = ByteSizeValue.ofBytes(1);
    private static final ByteSizeValue DEFAULT_TARGET_PAGE_SIZE = ByteSizeValue.ofBytes(Operator.TARGET_PAGE_SIZE);
    /** Every row reaches this target, so each output page holds exactly one row. */
    private static final ByteSizeValue ONE_ROW_PER_PAGE = ByteSizeValue.ofBytes(1);
    private static final List<String> THREE_SERIES = List.of(
        "{\"cpu_usage\": 0.5, \"host\": \"h1\"}",
        "{\"cpu_usage\": 0.5, \"host\": \"h2\"}",
        "{\"cpu_usage\": 0.5, \"host\": \"h3\"}"
    );
    /** Series with several dimensions, so that each merged row has a set with several values. */
    private static final List<String> WIDE_SERIES = List.of(
        "{\"cpu_usage\": 0.5, \"host\": \"h1\", \"az\": \"a\", \"rack\": \"r1\", \"region\": \"eu\", \"zone\": \"z1\"}",
        "{\"cpu_usage\": 0.5, \"host\": \"h2\", \"az\": \"a\", \"rack\": \"r1\", \"region\": \"eu\", \"zone\": \"z1\"}",
        "{\"cpu_usage\": 0.5, \"host\": \"h3\", \"az\": \"a\", \"rack\": \"r1\", \"region\": \"eu\", \"zone\": \"z1\"}"
    );

    private ByteSizeValue simplePartialEmitThreshold;

    @Override
    protected TsInfoOperator.Factory simple(SimpleOptions options) {
        ByteSizeValue targetPageSize = randomFrom(ONE_ROW_PER_PAGE, ByteSizeValue.ofBytes(between(2, 4096)), DEFAULT_TARGET_PAGE_SIZE);
        return initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, simplePartialEmitThreshold(), targetPageSize);
    }

    /** Chosen once per test so that {@link #simple} and its expected description agree. */
    private ByteSizeValue simplePartialEmitThreshold() {
        if (simplePartialEmitThreshold == null) {
            simplePartialEmitThreshold = randomFrom(
                EMIT_AFTER_EVERY_PAGE,
                ByteSizeValue.ofBytes(between(2, 4096)),
                TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD
            );
        }
        return simplePartialEmitThreshold;
    }

    private static TsInfoOperator.Factory initialFactory(
        MetricsInfoOperator.MetricFieldLookup lookup,
        Map<String, String> dataStreamsByIndex,
        ByteSizeValue partialEmitThreshold
    ) {
        return initialFactory(lookup, dataStreamsByIndex, partialEmitThreshold, DEFAULT_TARGET_PAGE_SIZE);
    }

    private static TsInfoOperator.Factory initialFactory(
        MetricsInfoOperator.MetricFieldLookup lookup,
        Map<String, String> dataStreamsByIndex,
        ByteSizeValue partialEmitThreshold,
        ByteSizeValue targetPageSize
    ) {
        return new TsInfoOperator.Factory(
            lookup,
            dataStreamsByIndex,
            METADATA_CHANNEL,
            INDEX_CHANNEL,
            partialEmitThreshold,
            targetPageSize
        );
    }

    private static TsInfoOperator.FinalFactory finalFactory(int limit) {
        return finalFactory(limit, DEFAULT_TARGET_PAGE_SIZE);
    }

    private static TsInfoOperator.FinalFactory finalFactory(int limit, ByteSizeValue targetPageSize) {
        return new TsInfoOperator.FinalFactory(FINAL_CHANNELS, limit, targetPageSize);
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
        return equalTo(
            "TsInfoOperator[mode=INITIAL, metadataSourceChannel=0, indexChannel=1, partialEmitThreshold="
                + simplePartialEmitThreshold()
                + "]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("TsInfoOperator[mode=INITIAL, partialEmitThreshold=" + simplePartialEmitThreshold() + "]");
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
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var totalInputRows = input.stream().mapToInt(Page::getPositionCount).sum();
        var totalOutputRows = output.stream().mapToInt(Page::getPositionCount).sum();
        assertMap(
            map,
            matchesMap().entry("mode", "INITIAL")
                .entry("pages_received", input.size())
                .entry("rows_received", totalInputRows)
                .entry("rows_emitted", totalOutputRows)
                .entry("entries_accumulated", greaterThanOrEqualTo(0))
        );
    }

    private Operator createInitialOperator() {
        return createInitialOperator(TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD);
    }

    private Operator createInitialOperator(ByteSizeValue partialEmitThreshold) {
        return initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, partialEmitThreshold).get(driverContext());
    }

    private Operator createFinalOperator(int[] channels) {
        return new TsInfoOperator.FinalFactory(channels, TsInfoOperator.NO_LIMIT, DEFAULT_TARGET_PAGE_SIZE).get(driverContext());
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
            assertTrue("Should contain host: " + dimensions, dimensions.contains("\"host\":\"server1\""));
            assertTrue("Should contain region: " + dimensions, dimensions.contains("\"region\":\"eu\""));

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

    /**
     * In cross-cluster search, the _index block contains a cluster-alias prefix
     * (e.g. "remote_cluster:my-index"). The operator preserves the prefix in the
     * data_stream column so users can distinguish remote data streams.
     */
    public void testRemoteClusterPrefixPreservedInDataStream() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.85, \"host\": \"server1\"}", "remote_cluster:my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "cpu_usage");
            assertColumnValue(output, 1, 0, "remote_cluster:my-index");
            assertColumnValue(output, 5, 0, "host");

            String dimensions = readSingleValue(output, 6, 0);
            assertNotNull(dimensions);
            assertTrue("Dimensions should contain host: " + dimensions, dimensions.contains("\"host\""));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    /**
     * When the _index block contains a cluster-prefixed backing index name
     * the supplied data-stream lookup preserves the cluster qualifier in the output.
     */
    public void testRemoteClusterPrefixPreservedForBackingIndex() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"a\"}", "remote_cluster:.ds-k8s-2024.01.15-000001");
            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"a\"}", "remote_cluster:.ds-k8s-2024.01.15-000002");
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "cpu_usage");
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("remote_cluster:k8s")));

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    /** Membership overrides naming conventions, while standalone indices retain their names. */
    public void testDataStreamMembershipLookup() {
        String clusterPrefix = randomBoolean() ? "remote_cluster:" : "";
        String customIndex = clusterPrefix + "custom-backing-index";
        String backingIndex = clusterPrefix + ".ds-misleading-name-2024.01.15-000001";
        String standaloneIndex = clusterPrefix + "partial-.ds-metrics-2024.01.15-000002";
        String dataStream = clusterPrefix + "metrics";
        Map<String, String> dataStreams = Map.of(customIndex, dataStream, backingIndex, dataStream);
        BlockFactory blockFactory = driverContext().blockFactory();
        try (Operator op = initialFactory(SIMPLE_LOOKUP, dataStreams, TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD).get(driverContext())) {
            for (String index : List.of(customIndex, backingIndex, standaloneIndex)) {
                op.addInput(buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"a\"}", index));
            }
            op.finish();
            Page output = op.getOutput();
            assertNotNull(output);
            try {
                assertThat(output.getPositionCount(), equalTo(1));
                assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of(dataStream, standaloneIndex)));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /** Local index names without a parent data stream pass through unchanged. */
    public void testLocalIndexNameUnchanged() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Operator op = createInitialOperator();
        try {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.85, \"host\": \"server1\"}", "my-plain-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "cpu_usage");
            assertColumnValue(output, 1, 0, "my-plain-index");

            output.releaseBlocks();
        } finally {
            op.close();
        }
    }

    public void testInitialModeTracksMemoryOnNewEntries() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();
        try (
            TsInfoOperator op = (TsInfoOperator) initialFactory(
                SIMPLE_LOOKUP,
                DATA_STREAMS_BY_INDEX,
                TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD
            ).get(ctx)
        ) {
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a");
            op.addInput(input1);
            long usedAfterOne = blockFactory.breaker().getUsed();
            assertThat(usedAfterOne, greaterThan(usedBefore));

            // Second distinct (metric, data stream, dimensions) adds another entry
            Page input2 = buildPage(blockFactory, "{\"disk_io\": 1024, \"host\": \"h1\"}", "index-a");
            op.addInput(input2);
            long usedAfterTwo = blockFactory.breaker().getUsed();
            assertThat(usedAfterTwo, greaterThan(usedAfterOne));

            // Same (cpu_usage, index-a, {"host":"h1"}) again → no new entry or set values
            Page input3 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"h1\"}", "index-a");
            op.addInput(input3);
            long usedAfterDuplicate = blockFactory.breaker().getUsed();
            assertThat(usedAfterDuplicate, equalTo(usedAfterTwo));

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
        try (TsInfoOperator op = (TsInfoOperator) finalFactory(TsInfoOperator.NO_LIMIT).get(ctx)) {
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
            assertThat(usedAfterOne, greaterThan(usedBefore));

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
            assertThat(usedAfterTwo, greaterThan(usedAfterOne));

            // Same (cpu_usage, ds-a, {}) with same set values → no new memory
            Page page3 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                "{}"
            );
            op.addInput(page3);
            long usedAfterDuplicate = blockFactory.breaker().getUsed();
            assertThat(usedAfterDuplicate, equalTo(usedAfterTwo));

            // Adding a new dimension field to an existing entry does track the new string
            Page page4 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host", "region"),
                "{}"
            );
            op.addInput(page4);
            long usedAfterNewSetValue = blockFactory.breaker().getUsed();
            assertThat(usedAfterNewSetValue, greaterThan(usedAfterDuplicate));

            op.finish();
            Page output = op.getOutput();
            assertNotNull(output);
            output.releaseBlocks();
        }
    }

    public void testFinalModeLimitsDistinctMetricDimensions() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (TsInfoOperator op = (TsInfoOperator) finalFactory(2).get(ctx)) {
            Page cpuA = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                "{}"
            );
            Page cpuB = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-b"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host"),
                "{}"
            );
            Page diskA = buildFinalPage(
                blockFactory,
                "disk_io",
                Set.of("ds-a"),
                Set.of("bytes"),
                Set.of("counter"),
                Set.of("long"),
                Set.of("host"),
                "{}"
            );
            Page memA = buildFinalPage(
                blockFactory,
                "memory",
                Set.of("ds-a"),
                Set.of("bytes"),
                Set.of("gauge"),
                Set.of("long"),
                Set.of("host"),
                "{}"
            );
            op.addInput(cpuA);
            op.addInput(new Page(3, concat(blockFactory, cpuB, diskA, memA)));
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(2));
            assertThat(readValues(output.getBlock(0), 0), equalTo(Set.of("cpu_usage")));
            assertThat(readValues(output.getBlock(0), 1), equalTo(Set.of("disk_io")));
            assertThat(readValues(output.getBlock(1), 0), equalTo(Set.of("ds-a", "ds-b")));
            assertThat(readValues(output.getBlock(1), 1), equalTo(Set.of("ds-a")));
            output.releaseBlocks();
            assertThat(op.toString(), equalTo("TsInfoOperator[mode=FINAL, limit=2]"));
            TsInfoOperator.Status status = (TsInfoOperator.Status) op.status();
            assertThat(status.rowsReceived(), equalTo(4L));
            assertThat(status.rowsEmitted(), equalTo(2L));
        }
    }

    private static Set<String> readValues(BytesRefBlock block, int position) {
        Set<String> values = new HashSet<>();
        int start = block.getFirstValueIndex(position);
        for (int i = start; i < start + block.getValueCount(position); i++) {
            values.add(block.getBytesRef(i, new BytesRef()).utf8ToString());
        }
        return values;
    }

    private static Block[] concat(BlockFactory blockFactory, Page... pages) {
        Block[] blocks = new Block[pages[0].getBlockCount()];
        try {
            for (int i = 0; i < blocks.length; i++) {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(pages.length)) {
                    for (Page page : pages) {
                        BytesRefBlock block = page.getBlock(i);
                        builder.copyFrom(block, 0, block.getPositionCount());
                    }
                    blocks[i] = builder.build();
                }
            }
            return blocks;
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
        }
    }

    public void testCloseReleasesTrackedMemory() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();

        TsInfoOperator op = (TsInfoOperator) initialFactory(
            SIMPLE_LOOKUP,
            DATA_STREAMS_BY_INDEX,
            TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD
        ).get(ctx);
        Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a");
        op.addInput(input);
        assertThat(blockFactory.breaker().getUsed(), greaterThan(usedBefore));

        op.finish();
        Page output = op.getOutput();
        assertNotNull(output);
        output.releaseBlocks();

        op.close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(usedBefore));
    }

    public void testInitialStopsTakingInputAtPartialEmitThreshold() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (Operator op = createInitialOperator(EMIT_AFTER_EVERY_PAGE)) {
            op.addInput(buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a"));

            assertFalse(op.needsInput());
            assertTrue(op.canProduceMoreDataWithoutExtraInput());
        }
    }

    public void testInitialEmitsBufferedRowsBeforeFinish() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (Operator op = createInitialOperator(EMIT_AFTER_EVERY_PAGE)) {
            op.addInput(buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a"));

            Page output = op.getOutput();

            assertNotNull(output);
            try {
                assertThat(output.getPositionCount(), equalTo(1));
                assertColumnValue(output, 0, 0, "cpu_usage");
                assertColumnValue(output, 1, 0, "index-a");
            } finally {
                output.releaseBlocks();
            }
            assertTrue(op.needsInput());
            assertFalse(op.isFinished());
        }
    }

    public void testInitialPartialEmitReturnsBufferedMemory() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();
        try (Operator op = initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, EMIT_AFTER_EVERY_PAGE).get(ctx)) {
            op.addInput(buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a"));
            assertThat(blockFactory.breaker().getUsed(), greaterThan(usedBefore));

            op.getOutput().releaseBlocks();

            assertThat(blockFactory.breaker().getUsed(), equalTo(usedBefore));
        }
    }

    public void testInitialBelowPartialEmitThresholdWaitsForFinish() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (Operator op = createInitialOperator(TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD)) {
            op.addInput(buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a"));

            assertTrue(op.needsInput());
            assertFalse(op.canProduceMoreDataWithoutExtraInput());
            assertNull(op.getOutput());
        }
    }

    public void testFinalNeverEmitsBeforeFinish() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (Operator op = createFinalOperator(FINAL_CHANNELS)) {
            for (String metric : List.of("cpu_usage", "disk_io")) {
                op.addInput(
                    buildFinalPage(blockFactory, metric, Set.of("ds-a"), Set.of("bytes"), Set.of("gauge"), Set.of("long"), Set.of(), "{}")
                );
            }

            assertTrue(op.needsInput());
            assertNull(op.getOutput());
        }
    }

    /**
     * The same series in two indices is merged into one row when INITIAL buffers both. Emitting early splits it
     * into two INITIAL rows, and FINAL must merge them back.
     */
    public void testFinalMergesRowsThatInitialEmittedEarly() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        String series = "{\"cpu_usage\": 0.5, \"host\": \"h1\"}";
        var runner = new TestDriverRunner().builder(driverContext)
            .input(buildPage(blockFactory, series, "index-a"), buildPage(blockFactory, series, "index-b"));

        List<Page> output = runner.run(
            initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, EMIT_AFTER_EVERY_PAGE),
            finalFactory(TsInfoOperator.NO_LIMIT)
        );

        try {
            TsInfoOperator.Status initialStatus = (TsInfoOperator.Status) runner.statuses().get(0);
            assertThat(initialStatus.rowsEmitted(), equalTo(2L));
            assertThat(output.stream().mapToInt(Page::getPositionCount).sum(), equalTo(1));
            Page merged = output.stream().filter(p -> p.getPositionCount() > 0).findFirst().orElseThrow();
            assertThat(collectMultiValues(merged, 1, 0), equalTo(Set.of("index-a", "index-b")));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /** A key emitted early and then seen again is a new entry, so it is counted again. */
    public void testStatusCountsEntriesEmittedEarly() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        String series = "{\"cpu_usage\": 0.5, \"host\": \"h1\"}";
        var runner = new TestDriverRunner().builder(driverContext)
            .input(buildPage(blockFactory, series, "index-a"), buildPage(blockFactory, series, "index-a"));

        List<Page> output = runner.run(initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, EMIT_AFTER_EVERY_PAGE));

        output.forEach(Page::releaseBlocks);
        TsInfoOperator.Status status = (TsInfoOperator.Status) runner.statuses().get(0);
        assertThat(status.entriesAccumulated(), equalTo(2));
    }

    public void testFactoryRejectsZeroPartialEmitThreshold() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, ByteSizeValue.ZERO)
        );
        assertThat(e.getMessage(), equalTo("partialEmitThreshold must be positive; got [0b]"));
    }

    public void testFinalFactoryRejectsZeroTargetPageSize() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> finalFactory(TsInfoOperator.NO_LIMIT, ByteSizeValue.ZERO)
        );
        assertThat(e.getMessage(), equalTo("targetPageSize must be positive; got [0b]"));
    }

    public void testInitialSplitsOutputIntoPagesOfTargetSize() {
        DriverContext driverContext = driverContext();
        var runner = new TestDriverRunner().builder(driverContext)
            .input(buildPageForIndex(driverContext.blockFactory(), "index-a", THREE_SERIES));

        List<Page> output = runner.run(
            initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD, ONE_ROW_PER_PAGE)
        );

        try {
            assertThat(output.stream().map(Page::getPositionCount).toList(), equalTo(List.of(1, 1, 1)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    public void testFinalSplitsOutputIntoPagesOfTargetSize() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = List.of("cpu_usage", "disk_io", "memory")
            .stream()
            .map(
                metric -> buildFinalPage(
                    blockFactory,
                    metric,
                    Set.of("ds-a"),
                    Set.of("bytes"),
                    Set.of("gauge"),
                    Set.of("long"),
                    Set.of("host"),
                    "{}"
                )
            )
            .toList();
        var runner = new TestDriverRunner().builder(driverContext).input(input);

        List<Page> output = runner.run(finalFactory(TsInfoOperator.NO_LIMIT, ONE_ROW_PER_PAGE));

        try {
            assertThat(output.stream().map(Page::getPositionCount).toList(), equalTo(List.of(1, 1, 1)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    public void testOutputBelowTargetPageSizeIsOnePage() {
        DriverContext driverContext = driverContext();
        var runner = new TestDriverRunner().builder(driverContext)
            .input(buildPageForIndex(driverContext.blockFactory(), "index-a", THREE_SERIES));

        List<Page> output = runner.run(initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD));

        try {
            assertThat(output.stream().map(Page::getPositionCount).toList(), equalTo(List.of(3)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    public void testNoInputWhileMergedRowsRemain() {
        DriverContext driverContext = driverContext();
        try (
            Operator op = initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, EMIT_AFTER_EVERY_PAGE, ONE_ROW_PER_PAGE).get(driverContext)
        ) {
            op.addInput(buildPageForIndex(driverContext.blockFactory(), "index-a", THREE_SERIES));

            op.getOutput().releaseBlocks();

            assertFalse(op.needsInput());
            assertTrue(op.canProduceMoreDataWithoutExtraInput());
        }
    }

    public void testNotFinishedUntilLastPageIsEmitted() {
        DriverContext driverContext = driverContext();
        try (
            Operator op = initialFactory(
                SIMPLE_LOOKUP,
                DATA_STREAMS_BY_INDEX,
                TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD,
                ONE_ROW_PER_PAGE
            ).get(driverContext)
        ) {
            op.addInput(buildPageForIndex(driverContext.blockFactory(), "index-a", THREE_SERIES.subList(0, 2)));
            op.finish();

            op.getOutput().releaseBlocks();
            assertFalse(op.isFinished());
            op.getOutput().releaseBlocks();

            assertTrue(op.isFinished());
        }
    }

    public void testBufferedMemoryReturnedAfterLastPage() {
        DriverContext driverContext = driverContext();
        long usedBefore = driverContext.breaker().getUsed();
        try (
            Operator op = initialFactory(
                SIMPLE_LOOKUP,
                DATA_STREAMS_BY_INDEX,
                TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD,
                ONE_ROW_PER_PAGE
            ).get(driverContext)
        ) {
            op.addInput(buildPageForIndex(driverContext.blockFactory(), "index-a", THREE_SERIES.subList(0, 2)));
            op.finish();

            op.getOutput().releaseBlocks();
            op.getOutput().releaseBlocks();

            assertThat(driverContext.breaker().getUsed(), equalTo(usedBefore));
        }
    }

    public void testEmittedRowsReturnTheirReservation() {
        DriverContext driverContext = driverContext();
        try (Operator op = oneRowPerPageOperator(driverContext)) {
            op.addInput(buildPageForIndex(driverContext.blockFactory(), "index-a", WIDE_SERIES));
            op.finish();
            op.getOutput().releaseBlocks();
            long usedAfterFirstRow = driverContext.breaker().getUsed();

            op.getOutput().releaseBlocks();

            assertThat(driverContext.breaker().getUsed(), lessThan(usedAfterFirstRow));
        }
    }

    private static Operator oneRowPerPageOperator(DriverContext driverContext) {
        return initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD, ONE_ROW_PER_PAGE).get(
            driverContext
        );
    }

    public void testNoEmptyPageAfterEarlyEmit() {
        DriverContext driverContext = driverContext();
        try (Operator op = initialFactory(SIMPLE_LOOKUP, DATA_STREAMS_BY_INDEX, EMIT_AFTER_EVERY_PAGE).get(driverContext)) {
            op.addInput(buildPage(driverContext.blockFactory(), "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a"));
            op.getOutput().releaseBlocks();
            op.finish();

            assertNull(op.getOutput());
            assertTrue(op.isFinished());
        }
    }

    private static Page buildPageForIndex(BlockFactory blockFactory, String indexName, List<String> metadataJsons) {
        try (
            BytesRefBlock.Builder metadataBuilder = blockFactory.newBytesRefBlockBuilder(metadataJsons.size());
            BytesRefBlock.Builder indexBuilder = blockFactory.newBytesRefBlockBuilder(metadataJsons.size())
        ) {
            for (String metadataJson : metadataJsons) {
                metadataBuilder.appendBytesRef(new BytesRef(metadataJson));
                indexBuilder.appendBytesRef(new BytesRef(indexName));
            }
            return new Page(metadataBuilder.build(), indexBuilder.build());
        }
    }

    /** Index abstractions of one series stay in separate rows when their metric metadata differs. */
    public void testFinalModeSplitsRowsWithDifferentMetadata() {
        BlockFactory factory = blockFactory();
        try (Operator op = createOperator(factory, true)) {
            op.addInput(
                buildFinalPage(factory, "cpu_usage", Set.of("ds-a"), Set.of("percent"), Set.of("gauge"), Set.of("double"), Set.of(), "{}")
            );
            op.addInput(
                buildFinalPage(factory, "cpu_usage", Set.of("ds-b"), Set.of("ratio"), Set.of("gauge"), Set.of("double"), Set.of(), "{}")
            );
            op.finish();
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(2));
                assertThat(readValues(output.getBlock(1), 0), equalTo(Set.of("ds-a")));
                assertThat(readValues(output.getBlock(2), 0), equalTo(Set.of("percent")));
                assertThat(readValues(output.getBlock(1), 1), equalTo(Set.of("ds-b")));
                assertThat(readValues(output.getBlock(2), 1), equalTo(Set.of("ratio")));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /** Multi-valued columns are sorted, so results do not depend on hash iteration order. */
    public void testMultiValuedColumnsAreSorted() {
        BlockFactory factory = blockFactory();
        try (Operator op = createOperator(factory, true)) {
            op.addInput(
                buildFinalPage(
                    factory,
                    "cpu_usage",
                    new LinkedHashSet<>(List.of("ds-c", "ds-a", "ds-b")),
                    new LinkedHashSet<>(List.of("ratio", "percent")),
                    Set.of("gauge"),
                    Set.of("double"),
                    new LinkedHashSet<>(List.of("region", "host", "az")),
                    "{}"
                )
            );
            op.finish();
            Page output = op.getOutput();
            try {
                assertThat(readOrderedValues(output.getBlock(1), 0), equalTo(List.of("ds-a", "ds-b", "ds-c")));
                assertThat(readOrderedValues(output.getBlock(2), 0), equalTo(List.of("percent", "ratio")));
                assertThat(readOrderedValues(output.getBlock(5), 0), equalTo(List.of("az", "host", "region")));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /** Uncharged state lets high-cardinality input exhaust the heap before the breaker trips. */
    public void testInitialModeChargesRetainedState() {
        assertChargesRetainedState(false);
    }

    /** The coordinator accumulates the series of every data node, so its state must be charged too. */
    public void testFinalModeChargesRetainedState() {
        assertChargesRetainedState(true);
    }

    /**
     * Compares the breaker charge with a lower bound of the operator's reachable state. The series table is charged by
     * {@link org.elasticsearch.common.util.BigArrays} itself, so it uses another breaker, and only the operator's own
     * charges are compared.
     */
    private void assertChargesRetainedState(boolean finalMode) {
        BlockFactory factory = blockFactory();
        try (Operator op = createOperator(new DriverContext(blockFactory().bigArrays(), factory, null), finalMode)) {
            long retainedBefore = RetainedOperatorState.bytes(op);
            long chargedBefore = factory.breaker().getUsed();
            for (int i = 0; i < 512; i++) {
                // Each host appears in three indices, so every series has several index abstractions.
                addSeries(op, factory, finalMode, "h" + (i / 3), "index-" + (i % 3));
            }
            long retained = RetainedOperatorState.bytes(op) - retainedBefore;
            long charged = factory.breaker().getUsed() - chargedBefore;
            assertThat(charged, greaterThanOrEqualTo(retained));
        }
    }

    /** Series of one metric reference its metadata in the mappings, so a large value is not charged once per series. */
    public void testSeriesShareMetadataValues() {
        String unit = randomAlphaOfLength(4096);
        MetricsInfoOperator.MetricFieldLookup lookup = (indexName, fieldName) -> fieldName.equals("cpu_usage")
            ? new MetricFieldInfo("cpu_usage", indexName, "double", "gauge", unit)
            : null;
        BlockFactory factory = blockFactory();
        DriverContext context = new DriverContext(factory.bigArrays(), factory, null);
        try (Operator op = initialFactory(lookup, Map.of(), TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD).get(context)) {
            op.addInput(buildPage(factory, "{\"cpu_usage\":0.5,\"host\":\"h0\"}", "index-a"));
            long usedAfterFirstSeries = factory.breaker().getUsed();
            int additionalSeries = 100;
            for (int i = 1; i <= additionalSeries; i++) {
                op.addInput(buildPage(factory, "{\"cpu_usage\":0.5,\"host\":\"h" + i + "\"}", "index-a"));
            }
            long bytesPerAdditionalSeries = (factory.breaker().getUsed() - usedAfterFirstSeries) / additionalSeries;
            assertThat(bytesPerAdditionalSeries, lessThan(RamUsageEstimator.sizeOf(unit)));
        }
    }

    /** The driver can keep a finished operator open, so its state must not outlive the output page. */
    public void testGetOutputReleasesAccumulatedState() {
        for (boolean finalMode : new boolean[] { false, true }) {
            BlockFactory factory = blockFactory();
            try (Operator op = createOperator(factory, finalMode)) {
                for (int i = 0; i < 16; i++) {
                    addSeries(op, factory, finalMode, "h" + i, "index-a");
                }
                op.finish();
                Page output = op.getOutput();
                assertThat(output.getPositionCount(), equalTo(16));
                output.releaseBlocks();
                assertThat(factory.breaker().getUsed(), equalTo(0L));
            }
        }
    }

    /** Coordinator memory pressure can interrupt accumulation before the operator reaches finish. */
    public void testFinalModeInputCircuitBreaking() {
        BlockFactory inputFactory = blockFactory();
        BlockFactory operatorFactory = blockFactory(ByteSizeValue.ofKb(randomIntBetween(1, 32)));
        expectThrows(CircuitBreakingException.class, () -> {
            try (Operator op = createOperator(operatorFactory, true)) {
                for (int i = 0; i < 512; i++) {
                    addSeries(op, inputFactory, true, "h" + i, "index-a");
                }
            }
        });
        assertThat(operatorFactory.breaker().getUsed(), equalTo(0L));
    }

    /** Allocation failures can interrupt a series or a canonical value half way through being recorded. */
    public void testFinalModeWithCrankyBreaker() {
        BlockFactory inputFactory = blockFactory();
        testWithCrankyBlockFactory(factory -> {
            DriverContext context = new DriverContext(factory.bigArrays(), factory, null);
            int limit = randomFrom(128, TsInfoOperator.NO_LIMIT);
            try (Operator op = finalFactory(limit).get(context)) {
                for (int i = 0; i < 512; i++) {
                    addSeries(op, inputFactory, true, "h" + i, "index-" + (i % 3));
                }
                op.finish();
                Page output = op.getOutput();
                output.releaseBlocks();
            }
        });
    }

    private Operator createOperator(BlockFactory factory, boolean finalMode) {
        return createOperator(new DriverContext(factory.bigArrays(), factory, null), finalMode);
    }

    private Operator createOperator(DriverContext context, boolean finalMode) {
        return finalMode
            ? finalFactory(TsInfoOperator.NO_LIMIT).get(context)
            : initialFactory(SIMPLE_LOOKUP, Map.of(), TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD).get(context);
    }

    private static void addSeries(Operator op, BlockFactory factory, boolean finalMode, String host, String index) {
        String dimensionsJson = "{\"host\":\"" + host + "\"}";
        if (finalMode) {
            op.addInput(
                buildFinalPage(
                    factory,
                    "cpu_usage",
                    Set.of(index),
                    Set.of("percent"),
                    Set.of("gauge"),
                    Set.of("double"),
                    Set.of("host"),
                    dimensionsJson
                )
            );
        } else {
            op.addInput(buildPage(factory, "{\"cpu_usage\":0.5,\"host\":\"" + host + "\"}", index));
        }
    }

    private static List<String> readOrderedValues(BytesRefBlock block, int position) {
        List<String> values = new ArrayList<>();
        int start = block.getFirstValueIndex(position);
        for (int i = start; i < start + block.getValueCount(position); i++) {
            values.add(block.getBytesRef(i, new BytesRef()).utf8ToString());
        }
        return values;
    }
}
