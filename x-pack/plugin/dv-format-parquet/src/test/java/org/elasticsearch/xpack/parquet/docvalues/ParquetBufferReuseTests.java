/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The reader keeps one buffer for the chunks of the row groups and one for the pages, so these tests read chunks and pages of very
 * different sizes, in an order that goes forward and backward, and check every value against what was written. A value that is read
 * from a buffer that a later read has overwritten would show here.
 */
public class ParquetBufferReuseTests extends StubSegmentTestCase {

    private static String string(BytesRef bytes) {
        return new String(bytes.bytes, bytes.offset, bytes.length, StandardCharsets.UTF_8);
    }

    public void testChunksAndPagesOfVeryDifferentSizesGiveTheRightValuesInAnyOrder() throws IOException {
        // writeStrings makes three row groups: huge values, tiny values, and values in between, so that the chunk buffer must grow,
        // shrink (it is swapped for a smaller one when it is more than four times too long) and grow again
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        int rowsPerGroup = 10;
        for (int i = 0; i < 3 * rowsPerGroup; i++) {
            required.add(switch (i / rowsPerGroup) {
                case 0 -> "huge-" + i + "-" + "x".repeat(150_000 + i);
                case 1 -> "t" + i;
                default -> "mid-" + i + "-" + "y".repeat(3_000 + i);
            });
            optional.add(i % 4 == 0 ? null : "opt-" + i + "-" + "z".repeat(i * 37));
        }
        // a value above the limit of a page buffer that a reader keeps (4 MB), so that this buffer is used once and let go
        required.set(rowsPerGroup + 3, "big-" + "w".repeat(4_500_000));
        for (boolean dictionary : new boolean[] { true, false }) {
            ParquetTestFiles.Options options = new ParquetTestFiles.Options(
                dictionary,
                randomFrom(256, 4096),
                1024 * 1024,
                randomFrom(CompressionCodecName.SNAPPY, CompressionCodecName.UNCOMPRESSED)
            );
            BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeStrings(required, optional, options));
            List<ColumnBinding> bindings = List.of(
                new ColumnBinding("s.req", 0, "req", DocValuesType.BINARY),
                new ColumnBinding("s.opt", 1, "opt", DocValuesType.BINARY)
            );
            withSegment(file, bindings, leaf -> {
                BinaryDocValues req = leaf.getBinaryDocValues("s.req");
                BinaryDocValues opt = leaf.getBinaryDocValues("s.opt");
                for (int round = 0; round < 300; round++) {
                    int doc = randomIntBetween(0, required.size() - 1);
                    assertTrue(req.advanceExact(doc));
                    assertEquals("row " + doc, required.get(doc), string(req.binaryValue()));
                    if (optional.get(doc) == null) {
                        assertFalse(opt.advanceExact(doc));
                    } else {
                        assertTrue(opt.advanceExact(doc));
                        assertEquals("row " + doc, optional.get(doc), string(opt.binaryValue()));
                    }
                }
                // a second reader over the same segment, in order, to read every page once
                BinaryDocValues sequential = leaf.getBinaryDocValues("s.req");
                for (int doc = 0; doc < required.size(); doc++) {
                    assertEquals(doc, sequential.nextDoc());
                    assertEquals("row " + doc, required.get(doc), string(sequential.binaryValue()));
                }
            });
        }
    }
}
