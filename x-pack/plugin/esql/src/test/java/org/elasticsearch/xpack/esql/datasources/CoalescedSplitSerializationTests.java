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

public class CoalescedSplitSerializationTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FileSplit.ENTRY, CoalescedSplit.ENTRY));

    public void testRoundTripWithFileSplitChildren() throws IOException {
        List<ExternalSplit> children = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/a.parquet"), 0, 1024, ".parquet", Map.of(), Map.of()),
            new FileSplit("file", StoragePath.of("s3://bucket/b.parquet"), 0, 2048, ".parquet", Map.of(), Map.of()),
            new FileSplit("file", StoragePath.of("s3://bucket/c.parquet"), 0, 512, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit original = new CoalescedSplit("file", children);

        CoalescedSplit deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals("file", deserialized.sourceType());
        assertEquals(3, deserialized.children().size());
        assertEquals(1024 + 2048 + 512, deserialized.estimatedSizeInBytes());
    }

    public void testRoundTripSingleChild() throws IOException {
        List<ExternalSplit> children = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/single.parquet"), 0, 4096, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit original = new CoalescedSplit("file", children);

        CoalescedSplit deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(1, deserialized.children().size());
        assertEquals(4096, deserialized.estimatedSizeInBytes());
    }

    public void testRoundTripNestedCoalesced() throws IOException {
        List<ExternalSplit> innerChildren = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/x.parquet"), 0, 100, ".parquet", Map.of(), Map.of()),
            new FileSplit("file", StoragePath.of("s3://bucket/y.parquet"), 0, 200, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit inner = new CoalescedSplit("file", innerChildren);

        List<ExternalSplit> outerChildren = List.of(
            inner,
            new FileSplit("file", StoragePath.of("s3://bucket/z.parquet"), 0, 300, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit original = new CoalescedSplit("file", outerChildren);

        CoalescedSplit deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(2, deserialized.children().size());
        assertTrue(deserialized.children().get(0) instanceof CoalescedSplit);
        assertTrue(deserialized.children().get(1) instanceof FileSplit);
        assertEquals(100 + 200 + 300, deserialized.estimatedSizeInBytes());
    }

    public void testNullSourceTypeThrows() {
        List<ExternalSplit> children = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/a.parquet"), 0, 100, ".parquet", Map.of(), Map.of())
        );
        expectThrows(IllegalArgumentException.class, () -> new CoalescedSplit(null, children));
    }

    public void testNullChildrenThrows() {
        expectThrows(IllegalArgumentException.class, () -> new CoalescedSplit("file", null));
    }

    public void testEmptyChildrenThrows() {
        expectThrows(IllegalArgumentException.class, () -> new CoalescedSplit("file", List.of()));
    }

    public void testEstimatedSizeWithZeroLengthChildren() throws IOException {
        List<ExternalSplit> children = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/a.parquet"), 0, 0, ".parquet", Map.of(), Map.of()),
            new FileSplit("file", StoragePath.of("s3://bucket/b.parquet"), 0, 0, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit original = new CoalescedSplit("file", children);

        assertEquals(0, original.estimatedSizeInBytes());

        CoalescedSplit deserialized = roundTrip(original);
        assertEquals(0, deserialized.estimatedSizeInBytes());
    }

    public void testWriteableName() {
        List<ExternalSplit> children = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/a.parquet"), 0, 100, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit split = new CoalescedSplit("file", children);
        assertEquals("CoalescedSplit", split.getWriteableName());
    }

    public void testToString() {
        List<ExternalSplit> children = List.of(
            new FileSplit("file", StoragePath.of("s3://bucket/a.parquet"), 0, 100, ".parquet", Map.of(), Map.of()),
            new FileSplit("file", StoragePath.of("s3://bucket/b.parquet"), 0, 200, ".parquet", Map.of(), Map.of())
        );
        CoalescedSplit split = new CoalescedSplit("file", children);
        String str = split.toString();
        assertTrue(str.contains("children=2"));
        assertTrue(str.contains("estimatedBytes=300"));
    }

    private CoalescedSplit roundTrip(CoalescedSplit original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        ExternalSplit deserialized = in.readNamedWriteable(ExternalSplit.class);
        assertTrue("Expected CoalescedSplit but got " + deserialized.getClass(), deserialized instanceof CoalescedSplit);
        return (CoalescedSplit) deserialized;
    }
}
