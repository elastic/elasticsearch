/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class DatasetTests extends AbstractXContentSerializingTestCase<Dataset> {

    @Override
    protected Dataset doParseInstance(XContentParser parser) throws IOException {
        return Dataset.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Dataset> instanceReader() {
        return Dataset::new;
    }

    @Override
    protected Dataset createTestInstance() {
        return randomDataset();
    }

    @Override
    protected Dataset mutateInstance(Dataset instance) {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> new Dataset(
                randomValueOtherThan(instance.name(), () -> randomAlphaOfLength(8).toLowerCase(Locale.ROOT)),
                instance.dataSource(),
                instance.resource(),
                instance.description(),
                instance.settings(),
                instance.mapping()
            );
            case 1 -> new Dataset(
                instance.name(),
                randomValueOtherThan(instance.dataSource(), () -> new DataSourceReference(randomAlphaOfLength(6).toLowerCase(Locale.ROOT))),
                instance.resource(),
                instance.description(),
                instance.settings(),
                instance.mapping()
            );
            case 2 -> new Dataset(
                instance.name(),
                instance.dataSource(),
                randomValueOtherThan(instance.resource(), () -> "s3://" + randomAlphaOfLength(6) + "/" + randomAlphaOfLength(6)),
                instance.description(),
                instance.settings(),
                instance.mapping()
            );
            case 3 -> new Dataset(
                instance.name(),
                instance.dataSource(),
                instance.resource(),
                randomValueOtherThan(instance.description(), () -> randomAlphaOfLengthBetween(1, 16)),
                instance.settings(),
                instance.mapping()
            );
            case 4 -> new Dataset(
                instance.name(),
                instance.dataSource(),
                instance.resource(),
                instance.description(),
                randomValueOtherThan(instance.settings(), DatasetTests::randomSettings),
                instance.mapping()
            );
            default -> new Dataset(
                instance.name(),
                instance.dataSource(),
                instance.resource(),
                instance.description(),
                instance.settings(),
                randomValueOtherThan(instance.mapping(), DatasetTests::randomMappingOrNull)
            );
        };
    }

    private static Dataset randomDataset() {
        return new Dataset(
            randomAlphaOfLength(8).toLowerCase(Locale.ROOT),
            new DataSourceReference(randomAlphaOfLength(6).toLowerCase(Locale.ROOT)),
            "s3://" + randomAlphaOfLength(8) + "/" + randomAlphaOfLength(6) + ".parquet",
            randomBoolean() ? null : randomAlphaOfLengthBetween(0, 32),
            randomSettings(),
            randomMappingOrNull()
        );
    }

    static DatasetMapping randomMappingOrNull() {
        return randomBoolean() ? null : randomMapping();
    }

    static DatasetMapping randomMapping() {
        // assemble() returns null when mappings is absent; always produce a mappings block so we get a non-null mapping.
        return DatasetMapping.assemble(randomMappings());
    }

    private static DatasetMapping.Mappings randomMappings() {
        DatasetMapping.Dynamic dynamic = randomFrom(DatasetMapping.Dynamic.values());
        int count = randomIntBetween(0, 4);
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            String type = randomFrom("keyword", "long", "integer", "double", "boolean", "date");
            String path = randomBoolean() ? null : randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            properties.put("col_" + i, new DatasetFieldMapping(type, path));
        }
        String idPath = randomBoolean() ? null : randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        return new DatasetMapping.Mappings(dynamic, properties, idPath);
    }

    private static Map<String, Object> randomSettings() {
        int count = randomIntBetween(0, 4);
        Map<String, Object> settings = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            settings.put(randomAlphaOfLength(6).toLowerCase(Locale.ROOT), randomFrom(randomAlphaOfLength(6), randomInt(), randomBoolean()));
        }
        return settings;
    }

    public void testIndexAbstractionType() {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertEquals(IndexAbstraction.Type.DATASET, dataset.getType());
    }

    public void testIndexAbstractionName() {
        var dataset = new Dataset("my_dataset", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertEquals("my_dataset", dataset.getName());
    }

    public void testIndexAbstractionNoIndices() {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertTrue(dataset.getIndices().isEmpty());
    }

    public void testIndexAbstractionNoWriteIndex() {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertNull(dataset.getWriteIndex());
    }

    public void testIndexAbstractionNoDataStream() {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertNull(dataset.getParentDataStream());
    }

    public void testNotHidden() {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertFalse(dataset.isHidden());
    }

    public void testNotSystem() {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertFalse(dataset.isSystem());
    }

    public void testRequiresName() {
        expectThrows(NullPointerException.class, () -> new Dataset(null, new DataSourceReference("ds"), "s3://b/p", null, Map.of()));
    }

    public void testRequiresDataSource() {
        expectThrows(NullPointerException.class, () -> new Dataset("test", null, "s3://b/p", null, Map.of()));
    }

    public void testRequiresResource() {
        expectThrows(NullPointerException.class, () -> new Dataset("test", new DataSourceReference("ds"), null, null, Map.of()));
    }

    public void testNullDescription() throws IOException {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        var deserialized = new Dataset(out.bytes().streamInput());
        assertNull(deserialized.description());
    }

    public void testEmptySettings() throws IOException {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        var deserialized = new Dataset(out.bytes().streamInput());
        assertTrue(deserialized.settings().isEmpty());
    }

    public void testWriteableRoundTripExplicit() throws IOException {
        var dataset = new Dataset(
            "access_logs",
            new DataSourceReference("my-s3"),
            "s3://bucket/logs/*.parquet",
            "Access logs dataset",
            Map.of("partition_detection", "hive", "schema_sample_size", 50)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        var deserialized = new Dataset(in);

        assertEquals(dataset, deserialized);
        assertEquals("access_logs", deserialized.getName());
        assertEquals("my-s3", deserialized.dataSource().getName());
        assertEquals("s3://bucket/logs/*.parquet", deserialized.resource());
        assertEquals("Access logs dataset", deserialized.description());
        assertEquals("hive", deserialized.settings().get("partition_detection"));
        assertEquals(50, deserialized.settings().get("schema_sample_size"));
    }

    public void testXContentRoundTripHeterogeneousSettings() throws IOException {
        // Covers all JSON-native value types inside the settings map (String, Integer, Long, Double, Boolean, null)
        // so the writeGenericValue / p.map() wire path is exercised end-to-end with the full type matrix.
        Map<String, Object> settings = new HashMap<>();
        settings.put("partition_detection", "hive");
        settings.put("schema_sample_size", 50);
        settings.put("max_file_size", 9_999_999_999L);
        settings.put("backoff_multiplier", 1.5);
        settings.put("case_insensitive", true);
        settings.put("optional_label", null);
        settings.put("error_mode", "skip_row");
        var dataset = new Dataset(
            "access_logs",
            new DataSourceReference("my-s3"),
            "s3://bucket/logs/*.parquet",
            "Access logs dataset",
            settings
        );
        assertExplicitXContentRoundTrip(dataset);
    }

    public void testXContentRoundTripNoDescription() throws IOException {
        var dataset = new Dataset(
            "access_logs",
            new DataSourceReference("my-s3"),
            "s3://bucket/logs/*.parquet",
            null,
            Map.of("partition_detection", "hive")
        );
        assertExplicitXContentRoundTrip(dataset);
    }

    public void testXContentRoundTripEmptySettings() throws IOException {
        var dataset = new Dataset("access_logs", new DataSourceReference("my-s3"), "s3://bucket/logs/*.parquet", "desc", Map.of());
        assertExplicitXContentRoundTrip(dataset);
    }

    public void testNoSchemaStaysNull() throws IOException {
        var dataset = new Dataset("test", new DataSourceReference("ds"), "s3://b/p", null, Map.of());
        assertNull(dataset.mapping());
        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        assertNull(new Dataset(out.bytes().streamInput()).mapping());
        // XContent of a no-mapping dataset must not emit any mapping keys
        XContentBuilder builder = JsonXContent.contentBuilder();
        dataset.toXContent(builder, null);
        String json = BytesReference.bytes(builder).utf8ToString();
        assertFalse(json.contains("mappings"));
        assertFalse(json.contains("id_field"));
    }

    public void testXContentRoundTripWithDeclaredSchema() throws IOException {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("when", new DatasetFieldMapping("date", "ts"));
        properties.put("status", new DatasetFieldMapping("integer", null));
        var mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties, "request_id"));
        var dataset = new Dataset(
            "access_logs",
            new DataSourceReference("my-s3"),
            "s3://bucket/logs/*.ndjson",
            "Access logs",
            Map.of("schema_sample_size", 20000),
            mapping
        );
        assertExplicitXContentRoundTrip(dataset);

        // wire round-trip too
        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        Dataset deserialized = new Dataset(out.bytes().streamInput());
        assertEquals(dataset, deserialized);
        assertEquals(DatasetMapping.Dynamic.FALSE, deserialized.mapping().mappings().dynamic());
        assertEquals("ts", deserialized.mapping().mappings().properties().get("when").path());
        assertEquals("request_id", deserialized.mapping().mappings().idPath());
    }

    public void testXContentRoundTripIdPathOnlyNoProperties() throws IOException {
        // _id.path with an otherwise-empty mappings block — the id-source is a meta-field, so it rides a mappings wrapper.
        var mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, Map.of(), "request_id"));
        var dataset = new Dataset("events", new DataSourceReference("s3"), "s3://b/*.ndjson", null, Map.of(), mapping);
        assertExplicitXContentRoundTrip(dataset);
        assertEquals("request_id", dataset.mapping().mappings().idPath());
    }

    private void assertExplicitXContentRoundTrip(Dataset dataset) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        dataset.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        Dataset deserialized = Dataset.fromXContent(parser);
        assertEquals(dataset, deserialized);
    }
}
