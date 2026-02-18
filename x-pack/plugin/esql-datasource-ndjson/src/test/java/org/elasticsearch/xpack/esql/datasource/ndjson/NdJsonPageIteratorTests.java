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
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
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
