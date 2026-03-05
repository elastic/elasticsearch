/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileSplitTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FileSplit.ENTRY));

    public void testConstruction() {
        StoragePath path = StoragePath.of("s3://bucket/data/year=2024/file.parquet");
        FileSplit split = new FileSplit("file", path, 0, 1024, ".parquet", Map.of("key", "val"), Map.of("year", 2024));

        assertEquals("file", split.sourceType());
        assertEquals(path, split.path());
        assertEquals(0, split.offset());
        assertEquals(1024, split.length());
        assertEquals(".parquet", split.format());
        assertEquals(Map.of("key", "val"), split.config());
        assertEquals(Map.of("year", 2024), split.partitionValues());
        assertEquals(1024, split.estimatedSizeInBytes());
    }

    public void testNullSourceTypeThrows() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        expectThrows(IllegalArgumentException.class, () -> new FileSplit(null, path, 0, 100, null, null, null));
    }

    public void testNullPathThrows() {
        expectThrows(IllegalArgumentException.class, () -> new FileSplit("file", null, 0, 100, null, null, null));
    }

    public void testNullConfigAndPartitionsDefaultToEmpty() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        FileSplit split = new FileSplit("file", path, 0, 100, null, null, null);
        assertEquals(Map.of(), split.config());
        assertEquals(Map.of(), split.partitionValues());
    }

    public void testNamedWriteableRoundTrip() throws IOException {
        StoragePath path = StoragePath.of("s3://bucket/data/year=2024/month=06/file.parquet");
        FileSplit original = new FileSplit(
            "file",
            path,
            100,
            2048,
            ".parquet",
            Map.of("endpoint", "https://s3.example.com"),
            Map.of("year", 2024, "month", 6)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        FileSplit deserialized = (FileSplit) in.readNamedWriteable(ExternalSplit.class);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals(original.sourceType(), deserialized.sourceType());
        assertEquals(original.path(), deserialized.path());
        assertEquals(original.offset(), deserialized.offset());
        assertEquals(original.length(), deserialized.length());
        assertEquals(original.format(), deserialized.format());
        assertEquals(original.config(), deserialized.config());
        assertEquals(original.partitionValues(), deserialized.partitionValues());
    }

    public void testNamedWriteableRoundTripMinimal() throws IOException {
        StoragePath path = StoragePath.of("s3://bucket/file.csv");
        FileSplit original = new FileSplit("file", path, 0, 500, null, Map.of(), Map.of());

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        FileSplit deserialized = (FileSplit) in.readNamedWriteable(ExternalSplit.class);

        assertEquals(original, deserialized);
    }

    public void testEquality() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        FileSplit a = new FileSplit("file", path, 0, 100, ".parquet", Map.of(), Map.of("year", 2024));
        FileSplit b = new FileSplit("file", path, 0, 100, ".parquet", Map.of(), Map.of("year", 2024));
        FileSplit c = new FileSplit("file", path, 0, 200, ".parquet", Map.of(), Map.of("year", 2024));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testGetWriteableName() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        FileSplit split = new FileSplit("file", path, 0, 100, null, null, null);
        assertEquals("FileSplit", split.getWriteableName());
    }

    public void testToString() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        FileSplit split = new FileSplit("file", path, 0, 100, null, null, Map.of("year", 2024));
        String str = split.toString();
        assertTrue(str.contains("s3://bucket/file.parquet"));
        assertTrue(str.contains("year"));
    }
}
