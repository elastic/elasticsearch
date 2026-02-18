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

public class MetricsInfoOperatorTests extends OperatorTestCase {

    /**
     * A simple lookup that recognizes "cpu_usage" and "disk_io" as metric fields.
     */
    private static final MetricsInfoOperator.MetricFieldLookup SIMPLE_LOOKUP = (indexName, fieldName) -> switch (fieldName) {
        case "cpu_usage" -> new MetricFieldInfo("cpu_usage", indexName, "double", "gauge", "percent");
        case "disk_io" -> new MetricFieldInfo("disk_io", indexName, "long", "counter", "bytes");
        default -> null;
    };

    private static final int METADATA_CHANNEL = 0;
    private static final int INDEX_CHANNEL = 1;

    @Override
    protected MetricsInfoOperator.Factory simple(SimpleOptions options) {
        return new MetricsInfoOperator.Factory(SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL);
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
        return equalTo("MetricsInfoOperator[mode=INITIAL, metadataSourceChannel=0, indexChannel=1]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("MetricsInfoOperator[mode=INITIAL]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
        // cpu_usage is the only metric; with 2 indices sharing the same signature they merge into 1 row
        assertThat(totalRows, equalTo(1));
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(MetricsInfoOperator.NUM_BLOCKS));
        }
    }

    @Override
    protected void assertStatus(@Nullable Map<String, Object> map, List<Page> input, List<Page> output) {
        assertNull(map);
    }

    public void testSingleMetricSingleIndex() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.85, \"host\": \"server1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertThat(output.getBlockCount(), equalTo(MetricsInfoOperator.NUM_BLOCKS));

            assertColumnValue(output, 0, 0, "cpu_usage");       // metric_name
            assertColumnValue(output, 1, 0, "my-index");        // data_stream
            assertColumnValue(output, 2, 0, "percent");          // unit
            assertColumnValue(output, 3, 0, "gauge");            // metric_type
            assertColumnValue(output, 4, 0, "double");           // field_type
            assertColumnValue(output, 5, 0, "host");             // dimension_fields

            output.releaseBlocks();
        }
    }

    public void testMultipleMetricsSameIndex() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Both cpu_usage and disk_io are metric fields
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"disk_io\": 1024, \"host\": \"server1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(2));

            Set<String> metricNames = collectColumnValues(output, 0);
            assertThat(metricNames, equalTo(Set.of("cpu_usage", "disk_io")));

            output.releaseBlocks();
        }
    }

    public void testSameMetricDifferentIndicesMergedBySignature() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Same metric from two different indices but with the same signature → merged into one row
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "index-a");
            op.addInput(input1);

            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"server2\"}", "index-b");
            op.addInput(input2);

            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Same metric name + same units/fieldTypes/metricTypes → merged into 1 row
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "cpu_usage"); // metric_name

            // data_stream should be multi-valued with both indices
            Set<String> dataStreams = collectMultiValues(output, 1, 0);
            assertThat(dataStreams, equalTo(Set.of("index-a", "index-b")));

            output.releaseBlocks();
        }
    }

    public void testDifferentSignaturesNotMerged() {
        BlockFactory blockFactory = driverContext().blockFactory();
        // Lookup returns different types for the same metric name in different indices
        try (MetricsInfoOperator op = getMetricsInfoOperator(blockFactory)) {
            Page input1 = buildPage(blockFactory, "{\"cpu\": 0.5}", "index-a");
            op.addInput(input1);

            Page input2 = buildPage(blockFactory, "{\"cpu\": 0.9}", "index-b");
            op.addInput(input2);

            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Different field_type (double vs float) → different signatures → 2 rows
            assertThat(output.getPositionCount(), equalTo(2));

            output.releaseBlocks();
        }
    }

    private static MetricsInfoOperator getMetricsInfoOperator(BlockFactory blockFactory) {
        MetricsInfoOperator.MetricFieldLookup lookup = (indexName, fieldName) -> {
            if ("cpu".equals(fieldName)) {
                if ("index-a".equals(indexName)) {
                    return new MetricFieldInfo("cpu", indexName, "double", "gauge", "percent");
                } else {
                    return new MetricFieldInfo("cpu", indexName, "float", "gauge", "percent");
                }
            }
            return null;
        };

        return new MetricsInfoOperator(blockFactory, lookup, METADATA_CHANNEL, INDEX_CHANNEL);
    }

    public void testDimensionFieldsCollected() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // "host" and "region" are non-metric (dimension) fields
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\", \"region\": \"us-east\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            Set<String> dimensions = collectMultiValues(output, 5, 0);
            assertThat(dimensions, equalTo(Set.of("host", "region")));

            output.releaseBlocks();
        }
    }

    public void testNestedMetadataFields() {
        BlockFactory blockFactory = driverContext().blockFactory();
        // Lookup that recognizes "system.cpu" as a metric with a dotted path
        MetricsInfoOperator.MetricFieldLookup lookup = (indexName, fieldName) -> {
            if ("system.cpu".equals(fieldName)) {
                return new MetricFieldInfo("system.cpu", indexName, "double", "gauge", "percent");
            }
            return null;
        };

        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, lookup, METADATA_CHANNEL, INDEX_CHANNEL)) {
            Page input = buildPage(blockFactory, "{\"system\": {\"cpu\": 0.75}, \"host\": \"server1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "system.cpu"); // metric_name
            assertColumnValue(output, 5, 0, "host");        // dimension_fields

            output.releaseBlocks();
        }
    }

    /**
     * When dimensions live under a passthrough-style path (e.g. resource.attributes.*), the operator
     * reports the full dotted path as the dimension key, not a shortened form like
     * "service.name". So we get "resource.attributes.service.name", and we do NOT get "service.name"
     * unless that path also exists as a leaf in the metadata.
     */
    public void testDimensionFieldsUnderPassthroughStylePath() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Metadata shaped like a passthrough: resource.attributes.service.name (dimension), cpu_usage (metric)
            String metadataJson = """
                {"resource": {"attributes": {"service": {"name": "api"}}}, "cpu_usage": 0.5}
                """;
            Page input = buildPage(blockFactory, metadataJson.trim(), "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "cpu_usage");
            Set<String> dimensions = collectMultiValues(output, 5, 0);
            // Full path is reported, not "service.name"
            assertThat(dimensions, equalTo(Set.of("resource.attributes.service.name")));

            output.releaseBlocks();
        }
    }

    /**
     * If both a top-level dimension path and a passthrough path exist as leaves (e.g. "service.name"
     * and "resource.attributes.service.name"), both appear in dimension_fields.
     */
    public void testBothTopLevelAndPassthroughDimensionPaths() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Two documents: one with top-level service.name, one with resource.attributes.service.name
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"service\": {\"name\": \"web\"}}", "my-index");
            Page input2 = buildPage(
                blockFactory,
                "{\"resource\": {\"attributes\": {\"service\": {\"name\": \"api\"}}}, \"cpu_usage\": 0.8}",
                "my-index"
            );
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            Set<String> dimensions = collectMultiValues(output, 5, 0);
            assertThat(dimensions, equalTo(Set.of("service.name", "resource.attributes.service.name")));

            output.releaseBlocks();
        }
    }

    public void testNullMetadataSkipped() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            try (
                BytesRefBlock.Builder metadataBuilder = blockFactory.newBytesRefBlockBuilder(2);
                BytesRefBlock.Builder indexBuilder = blockFactory.newBytesRefBlockBuilder(2)
            ) {
                // First row: null metadata
                metadataBuilder.appendNull();
                indexBuilder.appendBytesRef(new BytesRef("my-index"));

                // Second row: valid metadata
                metadataBuilder.appendBytesRef(new BytesRef("{\"cpu_usage\": 0.5, \"host\": \"server1\"}"));
                indexBuilder.appendBytesRef(new BytesRef("my-index"));

                Page input = new Page(metadataBuilder.build(), indexBuilder.build());
                op.addInput(input);
            }
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Only the second row contributes
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");

            output.releaseBlocks();
        }
    }

    public void testNullIndexSkipped() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            try (
                BytesRefBlock.Builder metadataBuilder = blockFactory.newBytesRefBlockBuilder(2);
                BytesRefBlock.Builder indexBuilder = blockFactory.newBytesRefBlockBuilder(2)
            ) {
                // First row: valid metadata, null index
                metadataBuilder.appendBytesRef(new BytesRef("{\"cpu_usage\": 0.5}"));
                indexBuilder.appendNull();

                // Second row: valid metadata, valid index
                metadataBuilder.appendBytesRef(new BytesRef("{\"cpu_usage\": 0.9, \"host\": \"server1\"}"));
                indexBuilder.appendBytesRef(new BytesRef("my-index"));

                Page input = new Page(metadataBuilder.build(), indexBuilder.build());
                op.addInput(input);
            }
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");

            output.releaseBlocks();
        }
    }

    public void testEmptyInputProducesEmptyPage() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(0));
            assertThat(output.getBlockCount(), equalTo(MetricsInfoOperator.NUM_BLOCKS));

            output.releaseBlocks();
        }
    }

    public void testGetOutputBeforeFinishReturnsNull() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            assertNull(op.getOutput());
            assertTrue(op.needsInput());
        }
    }

    public void testGetOutputCalledTwiceReturnsNullSecondTime() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "my-index");
            op.addInput(input);
            op.finish();

            Page output1 = op.getOutput();
            assertNotNull(output1);
            output1.releaseBlocks();

            Page output2 = op.getOutput();
            assertNull(output2);
            assertTrue(op.isFinished());
        }
    }

    public void testNeedsInputReturnsFalseAfterFinish() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            assertTrue(op.needsInput());
            op.finish();
            assertFalse(op.needsInput());
        }
    }

    public void testIsFinishedOnlyAfterOutputProduced() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            assertFalse(op.isFinished());

            op.finish();
            // finished=true but outputProduced=false → isFinished is still false
            assertFalse(op.isFinished());

            Page output = op.getOutput();
            assertNotNull(output);
            output.releaseBlocks();

            assertTrue(op.isFinished());
        }
    }

    public void testMetricWithNullUnit() {
        BlockFactory blockFactory = driverContext().blockFactory();
        MetricsInfoOperator.MetricFieldLookup lookup = (indexName, fieldName) -> {
            if ("temperature".equals(fieldName)) {
                return new MetricFieldInfo("temperature", indexName, "double", "gauge", null);
            }
            return null;
        };

        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, lookup, METADATA_CHANNEL, INDEX_CHANNEL)) {
            Page input = buildPage(blockFactory, "{\"temperature\": 36.6}", "my-index");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "temperature"); // metric_name
            // unit should be null
            BytesRefBlock unitBlock = output.getBlock(2);
            assertTrue(unitBlock.isNull(0));

            output.releaseBlocks();
        }
    }

    public void testInvalidJsonSkipped() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            try (
                BytesRefBlock.Builder metadataBuilder = blockFactory.newBytesRefBlockBuilder(2);
                BytesRefBlock.Builder indexBuilder = blockFactory.newBytesRefBlockBuilder(2)
            ) {
                // Row 1: invalid JSON
                metadataBuilder.appendBytesRef(new BytesRef("not-valid-json"));
                indexBuilder.appendBytesRef(new BytesRef("my-index"));

                // Row 2: valid JSON with a metric
                metadataBuilder.appendBytesRef(new BytesRef("{\"cpu_usage\": 0.5}"));
                indexBuilder.appendBytesRef(new BytesRef("my-index"));

                Page input = new Page(metadataBuilder.build(), indexBuilder.build());
                op.addInput(input);
            }
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Only the valid row contributes
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");

            output.releaseBlocks();
        }
    }

    public void testMultipleInputPages() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Page 1: cpu_usage from index-a
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "index-a");
            op.addInput(input1);

            // Page 2: cpu_usage + disk_io from index-b
            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.8, \"disk_io\": 2048, \"region\": \"eu\"}", "index-b");
            op.addInput(input2);

            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);

            Set<String> metricNames = collectColumnValues(output, 0);
            assertThat(metricNames, equalTo(Set.of("cpu_usage", "disk_io")));

            output.releaseBlocks();
        }
    }

    public void testDimensionFieldsAreUnionAcrossDocuments() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Document 1: dimension "host"
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "my-index");
            op.addInput(input1);

            // Document 2: dimension "region" (different dimension)
            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"region\": \"us-east\"}", "my-index");
            op.addInput(input2);

            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            // Dimensions should be union of both documents
            Set<String> dimensions = collectMultiValues(output, 5, 0);
            assertThat(dimensions, equalTo(Set.of("host", "region")));

            output.releaseBlocks();
        }
    }

    public void testFactoryDescribe() {
        MetricsInfoOperator.Factory factory = new MetricsInfoOperator.Factory(SIMPLE_LOOKUP, 3, 7);
        assertThat(factory.describe(), equalTo("MetricsInfoOperator[mode=INITIAL, metadataSourceChannel=3, indexChannel=7]"));
    }

    public void testToString() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, 0, 1)) {
            assertThat(op.toString(), equalTo("MetricsInfoOperator[mode=INITIAL]"));
        }
    }

    public void testResolveDataStreamName() {
        // Standard backing index format
        assertThat(MetricsInfoOperator.resolveDataStreamName(".ds-k8s-2024.01.15-000001"), equalTo("k8s"));
        assertThat(MetricsInfoOperator.resolveDataStreamName(".ds-my-stream-2024.01.15-000001"), equalTo("my-stream"));
        assertThat(MetricsInfoOperator.resolveDataStreamName(".ds-logs-2024-2024.06.30-000003"), equalTo("logs-2024"));
        // Failure store prefix
        assertThat(MetricsInfoOperator.resolveDataStreamName(".fs-my-stream-2024.01.15-000001"), equalTo("my-stream"));
        // Non-backing index → returned as-is
        assertThat(MetricsInfoOperator.resolveDataStreamName("my-index"), equalTo("my-index"));
        assertThat(MetricsInfoOperator.resolveDataStreamName("index-a"), equalTo("index-a"));
        // Malformed → returned as-is
        assertThat(MetricsInfoOperator.resolveDataStreamName(".ds-incomplete"), equalTo(".ds-incomplete"));
    }

    /**
     * Different metrics in the same tsid can have different dimension sets.
     * Metric A appears only with dim "host", metric B appears only with dim "mount".
     * After processing, A should have {host} and B should have {mount}, not the union.
     */
    public void testDifferentMetricsHaveDifferentDimensions() {
        BlockFactory blockFactory = driverContext().blockFactory();
        // Lookup: cpu_usage and disk_io are metrics
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Tsid 1: has cpu_usage + dimension "host"
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"server1\"}", "my-index");
            // Tsid 2: has disk_io + dimension "mount"
            Page input2 = buildPage(blockFactory, "{\"disk_io\": 1024, \"mount\": \"/data\"}", "my-index");
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(2));

            // Find which row is which metric
            for (int row = 0; row < output.getPositionCount(); row++) {
                BytesRefBlock nameBlock = output.getBlock(0);
                String metricName = nameBlock.getBytesRef(nameBlock.getFirstValueIndex(row), new BytesRef()).utf8ToString();
                Set<String> dims = collectMultiValues(output, 5, row);
                if ("cpu_usage".equals(metricName)) {
                    assertThat("cpu_usage should only have 'host' as dimension", dims, equalTo(Set.of("host")));
                } else if ("disk_io".equals(metricName)) {
                    assertThat("disk_io should only have 'mount' as dimension", dims, equalTo(Set.of("mount")));
                } else {
                    fail("Unexpected metric: " + metricName);
                }
            }

            output.releaseBlocks();
        }
    }

    /**
     * Backing indices of the same data stream are grouped together.
     * Two backing indices with the same metric and signature merge into one row
     * with the data stream name (not the backing index name) in the data_stream column.
     */
    public void testBackingIndicesSameDataStreamGroupedTogether() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Two backing indices of the same data stream "k8s"
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"a\"}", ".ds-k8s-2024.01.15-000001");
            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"b\"}", ".ds-k8s-2024.01.15-000002");
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Same metric + same signature + same data stream → 1 row
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");
            // data_stream should be the resolved data stream name, not the backing index name
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("k8s")));

            output.releaseBlocks();
        }
    }

    /**
     * Unit/metric_type/field_type become multi-valued when backing indices of the
     * same data stream have conflicting values.
     */
    public void testConflictingValuesWithinSameDataStreamProduceMultiValuedFields() {
        BlockFactory blockFactory = driverContext().blockFactory();
        // Lookup returns different field_type depending on backing index
        try (MetricsInfoOperator op = getInfoOperator(blockFactory)) {
            // Two backing indices of the same data stream "k8s", different field_type
            Page input1 = buildPage(blockFactory, "{\"cpu\": 0.5, \"host\": \"a\"}", ".ds-k8s-2024.01.15-000001");
            Page input2 = buildPage(blockFactory, "{\"cpu\": 0.9, \"host\": \"b\"}", ".ds-k8s-2024.01.15-000002");
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Same data stream → grouped into 1 MetricInfo → 1 row with multi-valued field_type
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu");
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("k8s")));
            // field_type should be multi-valued: {double, float}
            assertThat(collectMultiValues(output, 4, 0), equalTo(Set.of("double", "float")));

            output.releaseBlocks();
        }
    }

    private static MetricsInfoOperator getInfoOperator(BlockFactory blockFactory) {
        MetricsInfoOperator.MetricFieldLookup lookup = (indexName, fieldName) -> {
            if ("cpu".equals(fieldName)) {
                if (indexName.endsWith("-000001")) {
                    return new MetricFieldInfo("cpu", indexName, "double", "gauge", "percent");
                } else {
                    return new MetricFieldInfo("cpu", indexName, "float", "gauge", "percent");
                }
            }
            return null;
        };
        return new MetricsInfoOperator(blockFactory, lookup, METADATA_CHANNEL, INDEX_CHANNEL);
    }

    /**
     * Different data streams with different unit/metric_type/field_type produce
     * separate rows (one per data stream).
     */
    public void testDifferentDataStreamsDifferentValuesProduceSeparateRows() {
        BlockFactory blockFactory = driverContext().blockFactory();
        // Lookup returns different unit depending on index
        try (MetricsInfoOperator op = getOperator(blockFactory)) {
            Page input1 = buildPage(blockFactory, "{\"cpu\": 0.5, \"host\": \"a\"}", ".ds-k8s-2024.01.15-000001");
            Page input2 = buildPage(blockFactory, "{\"cpu\": 0.9, \"host\": \"b\"}", ".ds-other-2024.01.15-000001");
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Different unit (percent vs ratio) across different data streams → 2 rows
            assertThat(output.getPositionCount(), equalTo(2));

            // Verify each row has the correct data stream and unit
            for (int row = 0; row < output.getPositionCount(); row++) {
                Set<String> ds = collectMultiValues(output, 1, row);
                Set<String> units = collectMultiValues(output, 2, row);
                if (ds.contains("k8s")) {
                    assertThat(units, equalTo(Set.of("percent")));
                } else if (ds.contains("other")) {
                    assertThat(units, equalTo(Set.of("ratio")));
                } else {
                    fail("Unexpected data stream: " + ds);
                }
            }

            output.releaseBlocks();
        }
    }

    private static MetricsInfoOperator getOperator(BlockFactory blockFactory) {
        MetricsInfoOperator.MetricFieldLookup lookup = (indexName, fieldName) -> {
            if ("cpu".equals(fieldName)) {
                if (indexName.contains("k8s")) {
                    return new MetricFieldInfo("cpu", indexName, "double", "gauge", "percent");
                } else {
                    return new MetricFieldInfo("cpu", indexName, "double", "gauge", "ratio");
                }
            }
            return null;
        };
        return new MetricsInfoOperator(blockFactory, lookup, METADATA_CHANNEL, INDEX_CHANNEL);
    }

    /**
     * Different data streams that have the same values for unit/metric_type/field_type
     * produce one row with multi-valued data_stream.
     */
    public void testDifferentDataStreamsSameValuesProduceMultiValuedDataStream() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // Two different data streams, same metric with the same signature
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"a\"}", ".ds-k8s-2024.01.15-000001");
            Page input2 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"b\"}", ".ds-other-2024.01.15-000001");
            op.addInput(input1);
            op.addInput(input2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Same metric + same signature → 1 row with multi-valued data_stream
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("k8s", "other")));

            output.releaseBlocks();
        }
    }

    private static final int[] FINAL_CHANNELS = { 0, 1, 2, 3, 4, 5 };

    public void testFinalModeIdenticalRowsFromTwoDataNodesAreMerged() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            // Two data nodes produced identical rows for cpu_usage on index-a
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("index-a")));
            assertThat(collectMultiValues(output, 5, 0), equalTo(Set.of("host")));

            output.releaseBlocks();
        }
    }

    public void testFinalModePartiallyOverlappingDataStreamsAreMerged() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            // Data node 1 saw index-a and index-b; data node 2 saw index-a and index-c
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a", "index-b"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a", "index-c"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("index-a", "index-b", "index-c")));

            output.releaseBlocks();
        }
    }

    public void testFinalModeDifferentMetricNamesRemainSeparateRows() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "disk_io",
                Set.of("index-a"),
                Set.of("bytes"),
                Set.of("counter"),
                Set.of("long"),
                Set.of("host")
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(2));
            assertThat(collectColumnValues(output, 0), equalTo(Set.of("cpu_usage", "disk_io")));

            output.releaseBlocks();
        }
    }

    public void testFinalModeDifferentSignaturesProduceSeparateRows() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            // Same metric name but different field_type → different signatures → 2 rows
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu",
                Set.of("index-b"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("float"),
                Set.of("host")
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(2));

            output.releaseBlocks();
        }
    }

    public void testFinalModeEmptyInputProducesEmptyOutput() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(0));
            assertThat(output.getBlockCount(), equalTo(MetricsInfoOperator.NUM_BLOCKS));

            output.releaseBlocks();
        }
    }

    public void testFinalModeSingleDataNodeInputPassesThrough() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            Page page = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host", "region")
            );

            op.addInput(page);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "cpu_usage");
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("index-a")));
            assertThat(collectMultiValues(output, 2, 0), equalTo(Set.of("percent")));
            assertThat(collectMultiValues(output, 3, 0), equalTo(Set.of("gauge")));
            assertThat(collectMultiValues(output, 4, 0), equalTo(Set.of("double")));
            assertThat(collectMultiValues(output, 5, 0), equalTo(Set.of("host", "region")));

            output.releaseBlocks();
        }
    }

    public void testFinalModeDimensionFieldsAreUnionedAcrossDataNodes() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            // Data node 1 saw dimension "host"; data node 2 saw dimension "region"
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );
            Page page2 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("index-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("region")
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertThat(collectMultiValues(output, 5, 0), equalTo(Set.of("host", "region")));

            output.releaseBlocks();
        }
    }

    /**
     * Backing indices of the same data stream on different data nodes may report different units.
     * The FINAL phase must first merge by (metricName, dataStreamName) — unioning unit values —
     * before re-grouping by signature. Without this, rows with {usd} and {eur} would have
     * different signatures and produce two rows instead of one row with multi-valued unit.
     */
    public void testFinalModeSameDataStreamDifferentUnitsAcrossNodesProduceOneRow() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            // Data node A has backing index 000001 (unit=usd)
            Page page1 = buildFinalPage(
                blockFactory,
                "total_cost",
                Set.of("metrics-unit-conflict"),
                Set.of("usd"),
                Set.of("counter"),
                Set.of("double"),
                Set.of("metric.name")
            );
            // Data node B has backing indices 000002+000003 (unit=eur)
            Page page2 = buildFinalPage(
                blockFactory,
                "total_cost",
                Set.of("metrics-unit-conflict"),
                Set.of("eur"),
                Set.of("counter"),
                Set.of("double"),
                Set.of("metric.name")
            );

            op.addInput(page1);
            op.addInput(page2);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // One row with multi-valued unit, not two rows
            assertThat(output.getPositionCount(), equalTo(1));
            assertColumnValue(output, 0, 0, "total_cost");
            assertThat(collectMultiValues(output, 1, 0), equalTo(Set.of("metrics-unit-conflict")));
            assertThat(collectMultiValues(output, 2, 0), equalTo(Set.of("usd", "eur")));
            assertThat(collectMultiValues(output, 3, 0), equalTo(Set.of("counter")));
            assertThat(collectMultiValues(output, 4, 0), equalTo(Set.of("double")));
            assertThat(collectMultiValues(output, 5, 0), equalTo(Set.of("metric.name")));

            output.releaseBlocks();
        }
    }

    /**
     * Build a single-row 6-column page for FINAL mode input, matching the MetricsInfo output schema.
     */
    private static Page buildFinalPage(
        BlockFactory blockFactory,
        String metricName,
        Set<String> dataStreams,
        Set<String> units,
        Set<String> metricTypes,
        Set<String> fieldTypes,
        Set<String> dimensionFields
    ) {
        try (
            BytesRefBlock.Builder nameB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder dsB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder unitB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder mtB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder ftB = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder dfB = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            nameB.appendBytesRef(new BytesRef(metricName));
            appendSet(dsB, dataStreams);
            appendSet(unitB, units);
            appendSet(mtB, metricTypes);
            appendSet(ftB, fieldTypes);
            appendSet(dfB, dimensionFields);
            return new Page(nameB.build(), dsB.build(), unitB.build(), mtB.build(), ftB.build(), dfB.build());
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

    /**
     * A lookup that recognizes histogram-type fields in addition to scalar metrics.
     */
    private static final MetricsInfoOperator.MetricFieldLookup HISTOGRAM_LOOKUP = (indexName, fieldName) -> switch (fieldName) {
        case "histogram.legacy" -> new MetricFieldInfo("histogram.legacy", indexName, "histogram", "histogram", null);
        case "histogram.exponential" -> new MetricFieldInfo("histogram.exponential", indexName, "exponential_histogram", "histogram", null);
        case "histogram.tdigest" -> new MetricFieldInfo("histogram.tdigest", indexName, "tdigest", "histogram", null);
        case "cpu_usage" -> new MetricFieldInfo("cpu_usage", indexName, "double", "gauge", "percent");
        default -> null;
    };

    /**
     * A histogram metric whose synthetic source is a nested JSON object (values + counts)
     * must be recognized as a single metric, not recursed into and misclassified as dimensions.
     */
    public void testHistogramMetricRecognizedFromNestedSyntheticSource() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, HISTOGRAM_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            String metadataJson = """
                {"histogram": {"legacy": {"values": [1.0, 2.0, 3.0], "counts": [10, 20, 30]}}, "entity": {"id": "abc"}}""";
            Page input = buildPage(blockFactory, metadataJson.trim(), ".ds-histograms-2026.02.16-000001");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));
            assertThat(output.getBlockCount(), equalTo(MetricsInfoOperator.NUM_BLOCKS));

            assertColumnValue(output, 0, 0, "histogram.legacy");   // metric_name
            assertColumnValue(output, 3, 0, "histogram");           // metric_type
            assertColumnValue(output, 4, 0, "histogram");           // field_type

            // entity.id is the only dimension; histogram internal fields must not appear
            Set<String> dimensions = collectMultiValues(output, 5, 0);
            assertThat(dimensions, equalTo(Set.of("entity.id")));

            output.releaseBlocks();
        }
    }

    /**
     * An exponential histogram metric whose synthetic source is a deeply nested JSON object
     * (scale, sum, positive/negative buckets, etc.) must be recognized as a single metric.
     */
    public void testExponentialHistogramMetricRecognizedFromNestedSyntheticSource() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, HISTOGRAM_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            String metadataJson = """
                {"histogram": {"exponential": {"scale": 3, "sum": 42.5, "min": 1.0, "max": 10.0,\
                 "zero": {"count": 0, "threshold": 0.0},\
                 "positive": {"indices": [1, 2, 3], "counts": [5, 10, 15]},\
                 "negative": {"indices": [], "counts": []}}},\
                 "entity": {"id": "xyz"}}""";
            Page input = buildPage(blockFactory, metadataJson.trim(), ".ds-histograms-2026.02.16-000001");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "histogram.exponential"); // metric_name
            assertColumnValue(output, 3, 0, "histogram");              // metric_type
            assertColumnValue(output, 4, 0, "exponential_histogram");  // field_type

            // Only entity.id should be a dimension; exponential histogram internals must not leak
            Set<String> dimensions = collectMultiValues(output, 5, 0);
            assertThat(dimensions, equalTo(Set.of("entity.id")));

            output.releaseBlocks();
        }
    }

    /**
     * A tdigest metric whose synthetic source is a nested JSON object (centroids, counts, min, max, sum)
     * must be recognized as a single metric.
     */
    public void testTdigestMetricRecognizedFromNestedSyntheticSource() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, HISTOGRAM_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            String metadataJson = """
                {"histogram": {"tdigest": {"min": 0.5, "max": 99.5, "sum": 500.0,\
                 "centroids": [1.0, 50.0, 99.0], "counts": [100, 200, 100]}},\
                 "entity": {"id": "t1"}}""";
            Page input = buildPage(blockFactory, metadataJson.trim(), ".ds-histograms-2026.02.16-000001");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            assertThat(output.getPositionCount(), equalTo(1));

            assertColumnValue(output, 0, 0, "histogram.tdigest"); // metric_name
            assertColumnValue(output, 3, 0, "histogram");          // metric_type
            assertColumnValue(output, 4, 0, "tdigest");            // field_type

            Set<String> dimensions = collectMultiValues(output, 5, 0);
            assertThat(dimensions, equalTo(Set.of("entity.id")));

            output.releaseBlocks();
        }
    }

    /**
     * When a document contains both scalar metrics and histogram metrics, both types
     * are correctly recognized: scalar metrics as leaf values and histogram metrics
     * from their nested synthetic source structure.
     */
    public void testMixedScalarAndHistogramMetrics() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, HISTOGRAM_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // cpu_usage is a scalar metric (leaf), histogram.legacy is a complex metric (nested Map)
            String metadataJson = """
                {"cpu_usage": 0.75, "histogram": {"legacy": {"values": [1.0], "counts": [5]}}, "host": "srv1"}""";
            Page input = buildPage(blockFactory, metadataJson.trim(), ".ds-mixed-2026.02.16-000001");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Two distinct metrics: cpu_usage and histogram.legacy
            assertThat(output.getPositionCount(), equalTo(2));

            Set<String> metricNames = collectColumnValues(output, 0);
            assertThat(metricNames, equalTo(Set.of("cpu_usage", "histogram.legacy")));

            // Both metrics share the same dimension
            for (int row = 0; row < output.getPositionCount(); row++) {
                Set<String> dimensions = collectMultiValues(output, 5, row);
                assertThat(dimensions, equalTo(Set.of("host")));
            }

            output.releaseBlocks();
        }
    }

    /**
     * Dimensions coexisting with histogram metrics must still be correctly classified.
     * The histogram's internal fields (values, counts, centroids, etc.) must not leak
     * into the dimension_fields output.
     */
    public void testDimensionsNotContaminatedByHistogramInternalFields() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, HISTOGRAM_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            // All three histogram types in one document, plus a dimension
            String metadataJson = """
                {"histogram": {\
                "legacy": {"values": [1.0], "counts": [10]},\
                "exponential": {"scale": 2, "positive": {"indices": [1], "counts": [5]}},\
                "tdigest": {"centroids": [50.0], "counts": [100]}},\
                "entity": {"id": "all-types"}}""";
            Page input = buildPage(blockFactory, metadataJson.trim(), ".ds-histograms-2026.02.16-000001");
            op.addInput(input);
            op.finish();

            Page output = op.getOutput();
            assertNotNull(output);
            // Three histogram metrics
            assertThat(output.getPositionCount(), equalTo(3));

            Set<String> metricNames = collectColumnValues(output, 0);
            assertThat(metricNames, equalTo(Set.of("histogram.legacy", "histogram.exponential", "histogram.tdigest")));

            // Every row must have entity.id as the only dimension.
            // None of the histogram internals (values, counts, centroids, scale, positive, etc.)
            // should appear as dimension keys.
            for (int row = 0; row < output.getPositionCount(); row++) {
                Set<String> dimensions = collectMultiValues(output, 5, row);
                assertThat(
                    "row " + row + " should only have entity.id as dimension, got: " + dimensions,
                    dimensions,
                    equalTo(Set.of("entity.id"))
                );
            }

            output.releaseBlocks();
        }
    }

    /**
     * Build a single-row page with the given metadata JSON and index name.
     */
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

    /**
     * Read a single-valued BytesRef column value at the given row.
     */
    private static void assertColumnValue(Page page, int blockIndex, int position, String expected) {
        BytesRefBlock block = page.getBlock(blockIndex);
        if (block.getValueCount(position) == 1) {
            assertThat(block.getBytesRef(block.getFirstValueIndex(position), new BytesRef()).utf8ToString(), equalTo(expected));
        } else {
            // Multi-valued: check if expected is in the set
            Set<String> values = new HashSet<>();
            int start = block.getFirstValueIndex(position);
            int count = block.getValueCount(position);
            for (int i = start; i < start + count; i++) {
                values.add(block.getBytesRef(i, new BytesRef()).utf8ToString());
            }
            assertTrue("Expected [" + expected + "] in " + values, values.contains(expected));
        }
    }

    /**
     * Collect all distinct single-valued strings from a column across all rows.
     */
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

    /**
     * Collect all values from a multi-valued cell at a given position.
     */
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
        BlockFactory blockFactory = driverContext().blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL)) {
            Page input1 = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a");
            op.addInput(input1);
            long usedAfterOne = blockFactory.breaker().getUsed();
            assertThat(usedAfterOne - usedBefore, equalTo(MetricsInfoOperator.SHALLOW_SIZE));

            // Second distinct metric adds another entry
            Page input2 = buildPage(blockFactory, "{\"disk_io\": 1024, \"host\": \"h1\"}", "index-a");
            op.addInput(input2);
            long usedAfterTwo = blockFactory.breaker().getUsed();
            assertThat(usedAfterTwo - usedBefore, equalTo(2 * MetricsInfoOperator.SHALLOW_SIZE));

            // Same metric again from a different index but same data stream name → no new entry
            Page input3 = buildPage(blockFactory, "{\"cpu_usage\": 0.9, \"host\": \"h2\"}", "index-a");
            op.addInput(input3);
            long usedAfterDuplicate = blockFactory.breaker().getUsed();
            assertThat(usedAfterDuplicate - usedBefore, equalTo(2 * MetricsInfoOperator.SHALLOW_SIZE));

            op.finish();
            Page output = op.getOutput();
            assertNotNull(output);
            output.releaseBlocks();
        }
    }

    public void testFinalModeTracksMemoryOnNewEntries() {
        BlockFactory blockFactory = driverContext().blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();
        try (MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, FINAL_CHANNELS)) {
            Page page1 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host")
            );
            op.addInput(page1);
            long usedAfterOne = blockFactory.breaker().getUsed();
            assertThat(usedAfterOne - usedBefore, equalTo(MetricsInfoOperator.SHALLOW_SIZE));

            // Different metric name → new entry
            Page page2 = buildFinalPage(
                blockFactory,
                "disk_io",
                Set.of("ds-a"),
                Set.of("bytes"),
                Set.of("counter"),
                Set.of("long"),
                Set.of("host")
            );
            op.addInput(page2);
            long usedAfterTwo = blockFactory.breaker().getUsed();
            assertThat(usedAfterTwo - usedBefore, equalTo(2 * MetricsInfoOperator.SHALLOW_SIZE));

            // Same metric/data-stream again → no new entry
            Page page3 = buildFinalPage(
                blockFactory,
                "cpu_usage",
                Set.of("ds-a"),
                Set.of("percent"),
                Set.of("gauge"),
                Set.of("double"),
                Set.of("host", "region")
            );
            op.addInput(page3);
            long usedAfterDuplicate = blockFactory.breaker().getUsed();
            assertThat(usedAfterDuplicate - usedBefore, equalTo(2 * MetricsInfoOperator.SHALLOW_SIZE));

            op.finish();
            Page output = op.getOutput();
            assertNotNull(output);
            output.releaseBlocks();
        }
    }

    public void testCloseReleasesTrackedMemory() {
        BlockFactory blockFactory = driverContext().blockFactory();
        long usedBefore = blockFactory.breaker().getUsed();

        MetricsInfoOperator op = new MetricsInfoOperator(blockFactory, SIMPLE_LOOKUP, METADATA_CHANNEL, INDEX_CHANNEL);
        Page input = buildPage(blockFactory, "{\"cpu_usage\": 0.5, \"host\": \"h1\"}", "index-a");
        op.addInput(input);
        assertThat(blockFactory.breaker().getUsed() - usedBefore, equalTo(MetricsInfoOperator.SHALLOW_SIZE));

        op.finish();
        Page output = op.getOutput();
        assertNotNull(output);
        output.releaseBlocks();

        op.close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(usedBefore));
    }
}
