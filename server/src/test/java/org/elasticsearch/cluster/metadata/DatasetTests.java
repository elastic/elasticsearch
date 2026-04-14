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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

public class DatasetTests extends ESTestCase {

    public void testWriteableRoundTrip() throws IOException {
        var dataset = new Dataset(
            "access_logs",
            "my-s3",
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
        assertEquals("my-s3", deserialized.dataSource());
        assertEquals("s3://bucket/logs/*.parquet", deserialized.resource());
        assertEquals("Access logs dataset", deserialized.description());
        assertEquals("hive", deserialized.settings().get("partition_detection"));
        assertEquals(50, deserialized.settings().get("schema_sample_size"));
    }

    public void testIndexAbstractionType() {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        assertEquals(IndexAbstraction.Type.DATASET, dataset.getType());
    }

    public void testIndexAbstractionName() {
        var dataset = new Dataset("my_dataset", "ds", "s3://b/p", null, Map.of());
        assertEquals("my_dataset", dataset.getName());
    }

    public void testIndexAbstractionNoIndices() {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        assertTrue(dataset.getIndices().isEmpty());
    }

    public void testIndexAbstractionNoWriteIndex() {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        assertNull(dataset.getWriteIndex());
    }

    public void testIndexAbstractionNoDataStream() {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        assertNull(dataset.getParentDataStream());
    }

    public void testNotHidden() {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        assertFalse(dataset.isHidden());
    }

    public void testNotSystem() {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        assertFalse(dataset.isSystem());
    }

    public void testNullDescription() throws IOException {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        var deserialized = new Dataset(out.bytes().streamInput());
        assertNull(deserialized.description());
    }

    public void testEmptySettings() throws IOException {
        var dataset = new Dataset("test", "ds", "s3://b/p", null, Map.of());
        BytesStreamOutput out = new BytesStreamOutput();
        dataset.writeTo(out);
        var deserialized = new Dataset(out.bytes().streamInput());
        assertTrue(deserialized.settings().isEmpty());
    }

    public void testRequiresName() {
        expectThrows(NullPointerException.class, () -> new Dataset(null, "ds", "s3://b/p", null, Map.of()));
    }

    public void testRequiresDataSource() {
        expectThrows(NullPointerException.class, () -> new Dataset("test", null, "s3://b/p", null, Map.of()));
    }

    public void testRequiresResource() {
        expectThrows(NullPointerException.class, () -> new Dataset("test", "ds", null, null, Map.of()));
    }

    public void testXContentRoundTrip() throws IOException {
        var dataset = new Dataset(
            "access_logs",
            "my-s3",
            "s3://bucket/logs/*.parquet",
            "Access logs dataset",
            Map.of("partition_detection", "hive", "schema_sample_size", 50, "error_mode", "skip_row")
        );
        assertXContentRoundTrip(dataset);
    }

    public void testXContentRoundTripNoDescription() throws IOException {
        // description is optional and omitted from the serialized form when null — verify the parser handles the absence
        var dataset = new Dataset("access_logs", "my-s3", "s3://bucket/logs/*.parquet", null, Map.of("partition_detection", "hive"));
        assertXContentRoundTrip(dataset);
    }

    public void testXContentRoundTripEmptySettings() throws IOException {
        // settings is optional and omitted from the serialized form when empty — verify the parser handles the absence
        var dataset = new Dataset("access_logs", "my-s3", "s3://bucket/logs/*.parquet", "desc", Map.of());
        assertXContentRoundTrip(dataset);
    }

    private void assertXContentRoundTrip(Dataset dataset) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        dataset.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        Dataset deserialized = Dataset.fromXContent(parser);
        assertEquals(dataset, deserialized);
    }
}
