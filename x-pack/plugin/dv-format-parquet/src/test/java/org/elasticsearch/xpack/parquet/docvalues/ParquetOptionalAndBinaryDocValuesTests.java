/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Optional columns and byte array columns: the rows without a value are not documents, and the bytes of a string are its UTF-8. */
public class ParquetOptionalAndBinaryDocValuesTests extends StubSegmentTestCase {

    private static List<ColumnBinding> bindings() {
        return List.of(
            new ColumnBinding("f.maybe", 0, "maybe", DocValuesType.NUMERIC),
            new ColumnBinding("f.name", 1, "name", DocValuesType.BINARY),
            new ColumnBinding("f.kind", 2, "kind", DocValuesType.BINARY),
            new ColumnBinding("f.note", 3, "note", DocValuesType.BINARY),
            new ColumnBinding("f.low", 4, "low", DocValuesType.NUMERIC)
        );
    }

    private static String string(BytesRef bytes) {
        return new String(bytes.bytes, bytes.offset, bytes.length, StandardCharsets.UTF_8);
    }

    private static void assertOptionalNumeric(org.apache.lucene.index.LeafReader leaf, int rows) throws IOException {
        // maybe is i % 13 and missing for every third row
        NumericDocValues maybe = leaf.getNumericDocValues("f.maybe");
        int expectedDoc = 0;
        for (int doc = maybe.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = maybe.nextDoc()) {
            while (expectedDoc % 3 == 0) {
                expectedDoc++;
            }
            assertEquals(expectedDoc, doc);
            assertEquals(doc % 13, maybe.longValue());
            expectedDoc++;
        }
        while (expectedDoc < rows) {
            assertEquals("a document with a value was skipped", 0, expectedDoc % 3);
            expectedDoc++;
        }

        // advanceExact reports the rows without a value, and advance moves to the next row that has one
        maybe = leaf.getNumericDocValues("f.maybe");
        for (int doc = 0; doc < rows; doc += randomIntBetween(1, 17)) {
            if (doc % 3 == 0) {
                assertFalse("row " + doc + " has no value", maybe.advanceExact(doc));
            } else {
                assertTrue("row " + doc + " has a value", maybe.advanceExact(doc));
                assertEquals(doc % 13, maybe.longValue());
            }
        }
        NumericDocValues advanced = leaf.getNumericDocValues("f.maybe");
        for (int target = 0; target < rows; target += randomIntBetween(1, 40)) {
            int next = target;
            while (next < rows && next % 3 == 0) {
                next++;
            }
            int found = advanced.advance(target);
            if (next >= rows) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, found);
                break;
            }
            assertEquals(next, found);
        }
    }

    private static void assertStrings(org.apache.lucene.index.LeafReader leaf, int rows) throws IOException {
        BinaryDocValues name = leaf.getBinaryDocValues("f.name");
        BinaryDocValues kind = leaf.getBinaryDocValues("f.kind");
        BinaryDocValues note = leaf.getBinaryDocValues("f.note");
        int noteDoc = note.nextDoc();
        for (int doc = 0; doc < rows; doc++) {
            assertEquals(doc, name.nextDoc());
            assertEquals("n" + doc, string(name.binaryValue()));
            assertEquals(doc, kind.nextDoc());
            assertEquals("k" + doc % 5, string(kind.binaryValue()));
            if (doc % 4 == 0) {
                assertTrue("the note of row " + doc + " is missing", noteDoc != doc);
            } else {
                assertEquals(doc, noteDoc);
                assertEquals("note-" + doc % 9, string(note.binaryValue()));
                noteDoc = note.nextDoc();
            }
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, name.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, note.nextDoc());
    }

    public void testOptionalNumericAndStringsOverRandomWriterOptions() throws IOException {
        int rows = randomIntBetween(1, 2_000);
        int rowGroupRows = randomIntBetween(1, 700);
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(
            randomBoolean(),
            randomFrom(64, 256, 4096),
            randomFrom(256, 1024 * 1024),
            randomBoolean() ? CompressionCodecName.SNAPPY : CompressionCodecName.UNCOMPRESSED
        );
        logger.info("--> [{}] rows in row groups of [{}] with {}", rows, rowGroupRows, options);
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, rowGroupRows, options));
        withSegment(file, bindings(), leaf -> {
            assertOptionalNumeric(leaf, rows);
            assertStrings(leaf, rows);
        });
    }

    public void testDataPagesOfVersionTwoKeepTheLevelsApartFromTheCompressedValues() throws IOException {
        // version 2 pages use delta encodings for values that are not in a dictionary, so only the columns with few distinct
        // values are read: maybe, kind, note and low
        int rows = 1_500;
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(true, 256, 1024 * 1024, CompressionCodecName.SNAPPY).withV2();
        byte[] bytes = ParquetTestFiles.writeFlat(rows, 500, options);
        BytesParquetObject file = new BytesParquetObject(FILE, bytes);

        ColumnSnapshot maybe = ParquetFooterReader.read(file).piece(0, 1, 0, List.of("maybe")).columns().get(0);
        assertEquals(java.util.Set.of("RLE_DICTIONARY"), pageEncodings(bytes, maybe, 0));

        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("f.maybe", 0, "maybe", DocValuesType.NUMERIC),
            new ColumnBinding("f.kind", 2, "kind", DocValuesType.BINARY),
            new ColumnBinding("f.note", 3, "note", DocValuesType.BINARY),
            new ColumnBinding("f.low", 4, "low", DocValuesType.NUMERIC)
        );
        withSegment(file, bindings, leaf -> {
            assertOptionalNumeric(leaf, rows);
            BinaryDocValues kind = leaf.getBinaryDocValues("f.kind");
            BinaryDocValues note = leaf.getBinaryDocValues("f.note");
            NumericDocValues low = leaf.getNumericDocValues("f.low");
            int noteDoc = note.nextDoc();
            for (int doc = 0; doc < rows; doc++) {
                assertEquals(doc, kind.nextDoc());
                assertEquals("k" + doc % 5, string(kind.binaryValue()));
                assertEquals(doc, low.nextDoc());
                assertEquals(doc % 7, low.longValue());
                if (doc % 4 != 0) {
                    assertEquals(doc, noteDoc);
                    assertEquals("note-" + doc % 9, string(note.binaryValue()));
                    noteDoc = note.nextDoc();
                }
            }
        });
    }

    public void testStringsWithEmptyValuesUnicodeAndLargeValues() throws IOException {
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            required.add(switch (i % 6) {
                case 0 -> "";
                case 1 -> "plain";
                case 2 -> "héllo wörld ✓ " + i;
                case 3 -> randomAlphaOfLength(randomIntBetween(1, 50)) + "\u0000" + i;
                case 4 -> "x".repeat(100_000 + i);
                default -> "日本語-" + i;
            });
            optional.add(i % 5 == 0 ? null : (i % 7 == 0 ? "" : "opt-" + i));
        }
        for (boolean dictionary : new boolean[] { true, false }) {
            ParquetTestFiles.Options options = new ParquetTestFiles.Options(dictionary, 1024, 1024 * 1024, CompressionCodecName.SNAPPY);
            BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeStrings(required, optional, options));
            List<ColumnBinding> bindings = List.of(
                new ColumnBinding("s.req", 0, "req", DocValuesType.BINARY),
                new ColumnBinding("s.opt", 1, "opt", DocValuesType.BINARY)
            );
            withSegment(file, bindings, leaf -> {
                BinaryDocValues req = leaf.getBinaryDocValues("s.req");
                BinaryDocValues opt = leaf.getBinaryDocValues("s.opt");
                int optDoc = opt.nextDoc();
                for (int doc = 0; doc < required.size(); doc++) {
                    assertEquals(doc, req.nextDoc());
                    assertEquals(required.get(doc), string(req.binaryValue()));
                    if (optional.get(doc) == null) {
                        assertNotEquals(doc, optDoc);
                    } else {
                        assertEquals(doc, optDoc);
                        assertEquals(optional.get(doc), string(opt.binaryValue()));
                        optDoc = opt.nextDoc();
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, optDoc);
                // a seek to a row in the middle
                BinaryDocValues seek = leaf.getBinaryDocValues("s.req");
                assertTrue(seek.advanceExact(64));
                assertEquals(required.get(64), string(seek.binaryValue()));
            });
        }
    }
}
