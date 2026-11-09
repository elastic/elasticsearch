/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@code tryRead} gives ES|QL a block of values for a set of documents. Every result must be what the iterator gives one document at
 * a time, for any set of documents (dense, sparse, in runs, across pages and row groups), for every kind of builder that ES|QL asks
 * for, and for columns with and without missing values. The block factory here records what the builders receive.
 */
public class ParquetTryReadTests extends StubSegmentTestCase {

    private static final BlockDocValuesReader.ToDouble HALF = value -> value * 0.5;

    private static List<ColumnBinding> bindings() {
        return List.of(
            new ColumnBinding("f.id", 0, "id", DocValuesType.NUMERIC),
            new ColumnBinding("f.ts", 1, "ts", DocValuesType.NUMERIC),
            new ColumnBinding("f.low", 2, "low", DocValuesType.NUMERIC),
            new ColumnBinding("f.maybe", 3, "maybe", DocValuesType.NUMERIC),
            new ColumnBinding("f.name", 4, "name", DocValuesType.BINARY),
            new ColumnBinding("f.kind", 5, "kind", DocValuesType.BINARY),
            new ColumnBinding("f.note", 6, "note", DocValuesType.BINARY)
        );
    }

    /** What a builder was given, one entry for each document: the value, or {@code null} for a missing one. */
    record Values(List<Object> values) implements BlockLoader.Block {
        @Override
        public void close() {}
    }

    /** A builder that records what it receives, for every kind of builder that the factory hands out. */
    static class Recorder
        implements
            BlockLoader.SingletonLongBuilder,
            BlockLoader.SingletonIntBuilder,
            BlockLoader.SingletonDoubleBuilder,
            BlockLoader.SingletonBytesRefBuilder,
            BlockLoader.LongBuilder,
            BlockLoader.IntBuilder,
            BlockLoader.DoubleBuilder,
            BlockLoader.BytesRefBuilder {

        private final List<Object> values = new ArrayList<>();
        /** Whether this is a builder of ints, which takes the longs of a column and narrows them, as the real one does. */
        private final boolean ints;

        Recorder(boolean ints) {
            this.ints = ints;
        }

        @Override
        public Recorder appendLong(long value) {
            values.add(value);
            return this;
        }

        @Override
        public Recorder appendLongs(long[] longs, int from, int length) {
            for (int i = 0; i < length; i++) {
                values.add(ints ? (Object) Math.toIntExact(longs[from + i]) : (Object) longs[from + i]);
            }
            return this;
        }

        @Override
        public Recorder appendLongs(BlockDocValuesReader.ToDouble toDouble, long[] longs, int from, int length) {
            for (int i = 0; i < length; i++) {
                values.add(toDouble.convert(longs[from + i]));
            }
            return this;
        }

        @Override
        public Recorder appendInts(int[] ints, int from, int length) {
            for (int i = 0; i < length; i++) {
                values.add(ints[from + i]);
            }
            return this;
        }

        @Override
        public Recorder appendInt(int value) {
            values.add(value);
            return this;
        }

        @Override
        public Recorder appendDouble(double value) {
            values.add(value);
            return this;
        }

        @Override
        public Recorder appendBytesRef(BytesRef value) {
            values.add(new String(value.bytes, value.offset, value.length, StandardCharsets.UTF_8));
            return this;
        }

        @Override
        public Recorder appendBytesReference(BytesReference value) {
            return appendBytesRef(value.toBytesRef());
        }

        @Override
        public Recorder appendBytesRefs(byte[] bytes, int[] offsets) {
            // the real builder keeps these arrays, so they must be exactly the bytes of the values
            for (int i = 0; i + 1 < offsets.length; i++) {
                values.add(new String(bytes, offsets[i], offsets[i + 1] - offsets[i], StandardCharsets.UTF_8));
            }
            assertEquals("the array holds exactly the bytes of the values", offsets[offsets.length - 1], bytes.length);
            return this;
        }

        @Override
        public Recorder appendBytesRefs(byte[] bytes, int fixedLength) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Recorder appendNull() {
            values.add(null);
            return this;
        }

        @Override
        public Recorder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Recorder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Block build() {
            return new Values(values);
        }

        @Override
        public void close() {}
    }

    /** A kind of builder for each of the methods of the factory that {@code tryRead} uses. */
    private static BlockLoader.BlockFactory factory(List<Recorder> created) {
        return (BlockLoader.BlockFactory) Proxy.newProxyInstance(
            ParquetTryReadTests.class.getClassLoader(),
            new Class<?>[] { BlockLoader.BlockFactory.class },
            (proxy, method, args) -> switch (method.getName()) {
                case "singletonLongs", "singletonInts", "singletonDoubles", "singletonBytesRefs", "longsFromDocValues", "intsFromDocValues",
                    "doublesFromDocValues", "bytesRefsFromDocValues" -> {
                    Recorder recorder = new Recorder(method.getName().equals("singletonInts"));
                    created.add(recorder);
                    yield recorder;
                }
                default -> throw new UnsupportedOperationException(method.getName());
            }
        );
    }

    private static BlockLoader.Docs docs(int[] ids, boolean mayContainDuplicates) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return ids.length;
            }

            @Override
            public int get(int i) {
                return ids[i];
            }

            @Override
            public boolean mayContainDuplicates() {
                return mayContainDuplicates;
            }
        };
    }

    /** Some sets of documents: all of them, every n-th, runs of consecutive documents, and random ones, always in ascending order. */
    private static List<int[]> documentSets(int rows, java.util.function.IntPredicate has) {
        List<int[]> sets = new ArrayList<>();
        sets.add(java.util.stream.IntStream.range(0, rows).filter(has).toArray());
        int step = randomIntBetween(2, 40);
        sets.add(java.util.stream.IntStream.range(0, rows).filter(d -> d % step == 0).filter(has).toArray());
        sets.add(java.util.stream.IntStream.range(0, rows).filter(d -> (d / randomIntBetween(1, 50)) % 2 == 0).filter(has).toArray());
        int[] random = java.util.stream.IntStream.range(0, rows).filter(d -> randomInt(9) < 3).filter(has).toArray();
        sets.add(random);
        sets.add(new int[] { rows - 1 }.length > 0 && has.test(rows - 1) ? new int[] { rows - 1 } : new int[0]);
        return sets;
    }

    private static Object expectedNumeric(NumericDocValues reference, int doc, String kind) throws IOException {
        if (reference.advanceExact(doc) == false) {
            return null;
        }
        long value = reference.longValue();
        return switch (kind) {
            case "long" -> value;
            case "int" -> Math.toIntExact(value);
            case "double" -> HALF.convert(value);
            default -> throw new AssertionError(kind);
        };
    }

    private static BlockLoader.Block tryNumeric(
        LeafReader leaf,
        String field,
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        boolean nullsFiltered,
        String kind
    ) throws IOException {
        NumericDocValues values = leaf.getNumericDocValues(field);
        assertTrue("the iterator serves blocks", values instanceof BlockLoader.OptionalColumnAtATimeReader);
        return ((BlockLoader.OptionalColumnAtATimeReader) values).tryRead(
            factory,
            docs,
            offset,
            nullsFiltered,
            kind.equals("double") ? HALF : null,
            kind.equals("int"),
            false
        );
    }

    private static void assertNumeric(LeafReader leaf, String field, int rows, boolean required, boolean allowInt) throws IOException {
        for (String kind : allowInt ? new String[] { "long", "int", "double" } : new String[] { "long", "double" }) {
            for (int[] ids : documentSets(rows, d -> required || d % 3 != 0)) {
                if (ids.length == 0) {
                    continue;
                }
                int offset = randomBoolean() ? 0 : randomIntBetween(0, ids.length - 1);
                NumericDocValues reference = leaf.getNumericDocValues(field);
                List<Recorder> created = new ArrayList<>();
                // every document has a value here, so the caller may promise that
                BlockLoader.Block block = tryNumeric(leaf, field, factory(created), docs(ids, false), offset, randomBoolean(), kind);
                assertNotNull(block);
                List<Object> expected = new ArrayList<>();
                for (int i = offset; i < ids.length; i++) {
                    expected.add(expectedNumeric(reference, ids[i], kind));
                }
                assertEquals(field + " " + kind + " from " + offset + " of " + Arrays.toString(ids), expected, ((Values) block).values());
                assertEquals(1, created.size());
            }
        }
    }

    public void testNumericColumnsGiveTheValuesOfOneDocumentAtATime() throws IOException {
        int rows = randomIntBetween(1, 1_500);
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(
            randomBoolean(),
            randomFrom(64, 256, 4096),
            randomFrom(256, 1024 * 1024),
            randomBoolean() ? CompressionCodecName.SNAPPY : CompressionCodecName.UNCOMPRESSED
        );
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, randomIntBetween(1, 600), options));
        withSegment(file, bindings(), leaf -> {
            assertNumeric(leaf, "f.id", rows, true, true);
            assertNumeric(leaf, "f.low", rows, true, true);
            // the timestamps do not fit an int
            assertNumeric(leaf, "f.ts", rows, true, false);
        });
    }

    public void testAnOptionalNumericColumnGivesNullsForMissingValuesOnlyWhenTheCallerDoesNotPromiseAValue() throws IOException {
        int rows = randomIntBetween(1, 1_500);
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, randomIntBetween(1, 600)));
        withSegment(file, bindings(), leaf -> {
            // documents that all have a value, with the promise
            assertNumeric(leaf, "f.maybe", rows, false, true);
            // all documents, with no promise: the missing ones are nulls, and the same as the one at a time reader says
            for (String kind : new String[] { "long", "int", "double" }) {
                for (int[] ids : documentSets(rows, d -> true)) {
                    if (ids.length == 0) {
                        continue;
                    }
                    NumericDocValues reference = leaf.getNumericDocValues("f.maybe");
                    List<Object> expected = new ArrayList<>();
                    for (int id : ids) {
                        expected.add(expectedNumeric(reference, id, kind));
                    }
                    BlockLoader.Block block = tryNumeric(leaf, "f.maybe", factory(new ArrayList<>()), docs(ids, false), 0, false, kind);
                    assertNotNull(block);
                    assertEquals(expected, ((Values) block).values());
                }
            }
            // a promise that a document with no value breaks: the iterator does not read it, and the caller reads one at a time
            if (rows > 0) {
                assertNull(tryNumeric(leaf, "f.maybe", factory(new ArrayList<>()), docs(new int[] { 0 }, false), 0, true, "long"));
            }
        });
    }

    public void testDocumentsThatTheIteratorCannotReadInABlockAreLeftToTheCaller() throws IOException {
        int rows = 200;
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, 70));
        withSegment(file, bindings(), leaf -> {
            BlockLoader.BlockFactory factory = factory(new ArrayList<>());
            assertNull("duplicates", tryNumeric(leaf, "f.id", factory, docs(new int[] { 1, 2, 3 }, true), 0, true, "long"));
            assertNull("not in order", tryNumeric(leaf, "f.id", factory, docs(new int[] { 5, 3, 9 }, false), 0, true, "long"));
            assertNull("the same twice", tryNumeric(leaf, "f.id", factory, docs(new int[] { 5, 5 }, false), 0, true, "long"));
            assertNull("outside of the segment", tryNumeric(leaf, "f.id", factory, docs(new int[] { 5, rows }, false), 0, true, "long"));
            assertNull("nothing after the offset", tryNumeric(leaf, "f.id", factory, docs(new int[] { 5, 6 }, false), 2, true, "long"));
            BinaryDocValues strings = leaf.getBinaryDocValues("f.name");
            assertNull(
                "the format of a multi valued binary field",
                ((BlockLoader.OptionalColumnAtATimeReader) strings).tryRead(
                    factory,
                    docs(new int[] { 1 }, false),
                    0,
                    true,
                    null,
                    false,
                    true
                )
            );
        });
    }

    public void testTheIteratorStaysUsableAfterABlock() throws IOException {
        int rows = randomIntBetween(10, 1_000);
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, randomIntBetween(1, 300)));
        withSegment(file, bindings(), leaf -> {
            NumericDocValues values = leaf.getNumericDocValues("f.ts");
            int[] ids = java.util.stream.IntStream.range(0, rows).filter(d -> d % 2 == 0).toArray();
            assertNotNull(
                ((BlockLoader.OptionalColumnAtATimeReader) values).tryRead(
                    factory(new ArrayList<>()),
                    docs(ids, false),
                    0,
                    true,
                    null,
                    false,
                    false
                )
            );
            // forward, backward, and the same document again
            for (int doc : new int[] { rows - 1, 0, rows / 2, rows / 2, 1, rows - 1 }) {
                assertTrue(values.advanceExact(doc));
                assertEquals(doc * 1000L, values.longValue());
            }
        });
    }

    private static void assertStrings(LeafReader leaf, String field, int rows, boolean required) throws IOException {
        for (int[] ids : documentSets(rows, d -> required || d % 4 != 0)) {
            if (ids.length == 0) {
                continue;
            }
            int offset = randomBoolean() ? 0 : randomIntBetween(0, ids.length - 1);
            BinaryDocValues reference = leaf.getBinaryDocValues(field);
            List<Object> expected = new ArrayList<>();
            for (int i = offset; i < ids.length; i++) {
                if (reference.advanceExact(ids[i])) {
                    BytesRef value = reference.binaryValue();
                    expected.add(new String(value.bytes, value.offset, value.length, StandardCharsets.UTF_8));
                } else {
                    expected.add(null);
                }
            }
            BinaryDocValues values = leaf.getBinaryDocValues(field);
            assertTrue(values instanceof BlockLoader.OptionalColumnAtATimeReader);
            BlockLoader.Block block = ((BlockLoader.OptionalColumnAtATimeReader) values).tryRead(
                factory(new ArrayList<>()),
                docs(ids, false),
                offset,
                randomBoolean(),
                null,
                false,
                false
            );
            assertNotNull(block);
            assertEquals(field + " from " + offset + " of " + Arrays.toString(ids), expected, ((Values) block).values());
        }
    }

    public void testStringsGiveTheSameBytesAsOneDocumentAtATime() throws IOException {
        int rows = randomIntBetween(1, 1_500);
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(
            randomBoolean(),
            randomFrom(64, 256, 4096),
            randomFrom(256, 1024 * 1024),
            randomBoolean() ? CompressionCodecName.SNAPPY : CompressionCodecName.UNCOMPRESSED
        );
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeFlat(rows, randomIntBetween(1, 600), options));
        withSegment(file, bindings(), leaf -> {
            assertStrings(leaf, "f.name", rows, true);
            assertStrings(leaf, "f.kind", rows, true);
            // the optional column: documents that all have a value, and all documents with no promise from the caller
            assertStrings(leaf, "f.note", rows, false);
            BinaryDocValues reference = leaf.getBinaryDocValues("f.note");
            int[] all = java.util.stream.IntStream.range(0, rows).toArray();
            List<Object> expected = new ArrayList<>();
            for (int doc : all) {
                if (reference.advanceExact(doc)) {
                    expected.add(
                        new String(
                            reference.binaryValue().bytes,
                            reference.binaryValue().offset,
                            reference.binaryValue().length,
                            StandardCharsets.UTF_8
                        )
                    );
                } else {
                    expected.add(null);
                }
            }
            BlockLoader.Block block = ((BlockLoader.OptionalColumnAtATimeReader) leaf.getBinaryDocValues("f.note")).tryRead(
                factory(new ArrayList<>()),
                docs(all, false),
                0,
                false,
                null,
                false,
                false
            );
            assertEquals(expected, ((Values) block).values());
        });
    }

    public void testStringsOfVeryDifferentSizesFillOneArrayThatGrows() throws IOException {
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            required.add(i % 9 == 0 ? "x".repeat(100_000 + i) : (i % 3 == 0 ? "" : "v" + i));
            optional.add(i % 5 == 0 ? null : "o" + i);
        }
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(randomBoolean(), 1024, 1024 * 1024, CompressionCodecName.SNAPPY);
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeStrings(required, optional, options));
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("s.req", 0, "req", DocValuesType.BINARY),
            new ColumnBinding("s.opt", 1, "opt", DocValuesType.BINARY)
        );
        withSegment(file, bindings, leaf -> {
            int[] all = java.util.stream.IntStream.range(0, required.size()).toArray();
            BlockLoader.Block block = ((BlockLoader.OptionalColumnAtATimeReader) leaf.getBinaryDocValues("s.req")).tryRead(
                factory(new ArrayList<>()),
                docs(all, false),
                0,
                true,
                null,
                false,
                false
            );
            assertEquals(new ArrayList<Object>(required), ((Values) block).values());
        });
    }
}
