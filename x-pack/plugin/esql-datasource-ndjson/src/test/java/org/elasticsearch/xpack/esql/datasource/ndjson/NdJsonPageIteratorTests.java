/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class NdJsonPageIteratorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testIterator() throws IOException {
        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("classpath://employees.ndjson", IOUtils.resourceToByteArray("/employees.ndjson"));

        List<Integer> sizes = new ArrayList<>();
        try (var iterator = reader.read(object, List.of("still_hired", "emp_no", "birth_date", "non_existing_field"), 42)) {
            while (iterator.hasNext()) {
                var page = iterator.next();
                assertEquals(4, page.getBlockCount());
                checkBlockSizes(page);

                // Make sure blocks are returned in the order requested, with nulls for unknown columns
                assertThat(page.getBlock(0), Matchers.instanceOf(BooleanBlock.class));
                assertThat(page.getBlock(1), Matchers.instanceOf(IntBlock.class));
                assertThat(page.getBlock(2), Matchers.instanceOf(LongBlock.class));
                assertThat(page.getBlock(3), Matchers.instanceOf(ConstantNullBlock.class));

                sizes.add(page.getBlock(0).getPositionCount());
            }
        }

        assertEquals(List.of(42, 42, 16), sizes); // Total 100
    }

    public void testJsonExtensionRecognized() throws IOException {
        var reader = new NdJsonFormatReader(blockFactory);
        assertTrue("NdJsonFormatReader should list .json as a supported extension", reader.fileExtensions().contains(".json"));
    }

    public void testJsonExtensionReadsData() throws IOException {
        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///data.json", IOUtils.resourceToByteArray("/employees.ndjson"));

        try (var iterator = reader.read(object, List.of("emp_no"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertThat(page.getBlock(0), Matchers.instanceOf(IntBlock.class));
            assertTrue(page.getPositionCount() > 0);
        }
    }

    public void testSkipFirstLineForSplit() throws IOException {
        // Simulate a split that starts mid-line: "partial_first_line\n{\"id\":1}\n{\"id\":2}\n"
        String data = "partial_first_line\n{\"id\":1}\n{\"id\":2}\n";
        var object = new BytesStorageObject("file:///split.ndjson", data.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        var reader = new NdJsonFormatReader(blockFactory);
        try (var iterator = reader.readSplit(object, List.of("id"), 100, true, null)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            // Should have skipped "partial_first_line" and read 2 records
            assertEquals(2, page.getPositionCount());
            assertThat(page.getBlock(0), Matchers.instanceOf(IntBlock.class));
            IntBlock idBlock = page.getBlock(0);
            assertEquals(1, idBlock.getInt(0));
            assertEquals(2, idBlock.getInt(1));
        }
    }

    public void testSkipFirstLineNoSkip() throws IOException {
        String data = "{\"id\":1}\n{\"id\":2}\n";
        var object = new BytesStorageObject("file:///split.ndjson", data.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        var reader = new NdJsonFormatReader(blockFactory);
        try (var iterator = reader.readSplit(object, List.of("id"), 100, false, null)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(2, page.getPositionCount());
        }
    }

    public void testSampleData() throws Exception {
        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("classpath://employees.ndjson", IOUtils.resourceToByteArray("/employees.ndjson"));

        var metadata = reader.metadata(object);
        var schema = metadata.schema();

        assertEquals("birth_date", schema.get(0).name());
        assertEquals(DataType.DATETIME, schema.get(0).dataType());

        assertEquals("emp_no", schema.get(1).name());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());

        assertEquals("still_hired", schema.get(9).name());
        assertEquals(DataType.BOOLEAN, schema.get(9).dataType());

        try (var iterator = reader.read(object, List.of(), 1000)) {
            var page = iterator.next();
            checkBlockSizes(page);

            LongBlock birthDate = page.getBlock(blockIdx(metadata, "birth_date"));
            IntBlock empNo = page.getBlock(blockIdx(metadata, "emp_no"));
            BooleanBlock stillHired = page.getBlock(blockIdx(metadata, "still_hired"));
            DoubleBlock height = page.getBlock(blockIdx(metadata, "height"));

            var bytesRef = new BytesRef();

            assertEquals("1963-06-01T00:00:00Z", Instant.ofEpochMilli(birthDate.getLong(9)).toString());
            assertEquals(10010, empNo.getInt(9));
            assertFalse(stillHired.getBoolean(9));
            assertEquals(1.70, height.getDouble(9), 0.0001);
        }
    }

    public void testMalformedLineDoesNotCrash() throws IOException {
        // A completely invalid JSON line should not crash the parser; it should be skipped
        String ndjson = "{\"name\":\"alice\",\"age\":30}\n" + "NOT-JSON-AT-ALL\n" + "{\"name\":\"charlie\",\"age\":40}\n";
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(blockFactory);

        List<Page> pages = new ArrayList<>();
        try (var iterator = reader.read(object, List.of(), 100)) {
            while (iterator.hasNext()) {
                pages.add(iterator.next());
            }
        }

        // Should produce at least 2 rows (alice + charlie); the invalid line is handled gracefully
        int totalRows = 0;
        for (var page : pages) {
            totalRows += page.getPositionCount();
            checkBlockSizes(page);
        }
        assertTrue("Should produce at least the valid rows", totalRows >= 2);
    }

    public void testConsistentBlockPositionCounts() throws IOException {
        // Ensures all blocks in a page have the same position count even with malformed data
        String ndjson = "{\"x\":1,\"y\":\"a\"}\n" + "{\"x\":2}\n" + "{\"x\":3,\"y\":\"c\"}\n";
        var object = new BytesStorageObject("memory://test.ndjson", ndjson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(blockFactory);

        try (var iterator = reader.read(object, List.of(), 100)) {
            while (iterator.hasNext()) {
                var page = iterator.next();
                checkBlockSizes(page);
                assertEquals(3, page.getPositionCount());
            }
        }
    }

    private int blockIdx(SourceMetadata meta, String name) {
        for (int i = 0; i < meta.schema().size(); i++) {
            if (meta.schema().get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column '" + name + "' not found in metadata");
    }

    private void checkBlockSizes(Page page) {
        int size = page.getPositionCount();
        for (int i = 0; i < page.getBlockCount(); i++) {
            assertEquals("Block[" + i + "] position count", size, page.getBlock(i).getPositionCount());
        }
    }
}
