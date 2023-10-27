/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.BlockLoader.BooleanBuilder;
import org.elasticsearch.index.mapper.BlockLoader.Builder;
import org.elasticsearch.index.mapper.BlockLoader.BuilderFactory;
import org.elasticsearch.index.mapper.BlockLoader.BytesRefBuilder;
import org.elasticsearch.index.mapper.BlockLoader.Docs;
import org.elasticsearch.index.mapper.BlockLoader.DoubleBuilder;
import org.elasticsearch.index.mapper.BlockLoader.IntBuilder;
import org.elasticsearch.index.mapper.BlockLoader.LongBuilder;

import java.io.IOException;

/**
 * A reader that supports reading doc-values from a Lucene segment in Block fashion.
 */
public abstract class BlockDocValuesReader {
    public interface Factory {
        BlockDocValuesReader build(int segment) throws IOException;

        boolean supportsOrdinals();

        SortedSetDocValues ordinals(int segment) throws IOException;
    }

    protected final Thread creationThread;

    public BlockDocValuesReader() {
        this.creationThread = Thread.currentThread();
    }

    /**
     * Returns the current doc that this reader is on.
     */
    public abstract int docID();

    /**
     * The {@link BlockLoader.Builder} for data of this type.
     */
    public abstract Builder builder(BuilderFactory factory, int expectedCount);

    /**
     * Reads the values of the given documents specified in the input block
     */
    public abstract BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException;

    /**
     * Reads the values of the given document into the builder
     */
    public abstract void readValuesFromSingleDoc(int docId, Builder builder) throws IOException;

    /**
     * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
     */
    public static boolean canReuse(BlockDocValuesReader reader, int startingDocID) {
        return reader != null && reader.creationThread == Thread.currentThread() && reader.docID() <= startingDocID;
    }

    public static BlockLoader booleans(String fieldName) {
        return context -> {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonBooleans(singleton);
            }
            return new Booleans(docValues);
        };
    }

    public static BlockLoader bytesRefsFromOrds(String fieldName) {
        return new BlockLoader() {
            @Override
            public BlockDocValuesReader reader(LeafReaderContext context) throws IOException {
                SortedSetDocValues docValues = ordinals(context);
                SortedDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return new SingletonOrdinals(singleton);
                }
                return new Ordinals(docValues);
            }

            @Override
            public boolean supportsOrdinals() {
                return true;
            }

            @Override
            public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
                return DocValues.getSortedSet(context.reader(), fieldName);
            }
        };
    }

    /**
     * Load {@link BytesRef} values from doc values. Prefer {@link #bytesRefsFromOrds} if
     * doc values are indexed with ordinals because that's generally much faster. It's
     * possible to use this with field data, but generally should be avoided because field
     * data has higher per invocation overhead.
     */
    public static BlockLoader bytesRefsFromDocValues(CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> fieldData) {
        return context -> new Bytes(fieldData.apply(context));
    }

    /**
     * Convert from the stored {@link long} into the {@link double} to load.
     * Sadly, this will go megamorphic pretty quickly and slow us down,
     * but it gets the job done for now.
     */
    public interface ToDouble {
        double convert(long v);
    }

    /**
     * Load {@code double} values from doc values.
     */
    public static BlockLoader doubles(String fieldName, ToDouble toDouble) {
        return context -> {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonDoubles(singleton, toDouble);
            }
            return new Doubles(docValues, toDouble);
        };
    }

    /**
     * Load {@code int} values from doc values.
     */
    public static BlockLoader ints(String fieldName) {
        return context -> {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonInts(singleton);
            }
            return new Ints(docValues);
        };
    }

    /**
     * Load a block of {@code long}s from doc values.
     */
    public static BlockLoader longs(String fieldName) {
        return context -> {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonLongs(singleton);
            }
            return new Longs(docValues);
        };
    }

    /**
     * Load blocks with only null.
     */
    public static BlockLoader nulls() {
        return context -> new Nulls();
    }

    @Override
    public abstract String toString();

    private static class SingletonLongs extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonLongs(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.LongBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.longsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.LongBuilder builder = builder(factory, docs.count())) {
                int lastDoc = -1;
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendLong(numericDocValues.longValue());
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            BlockLoader.LongBuilder blockBuilder = (BlockLoader.LongBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendLong(numericDocValues.longValue());
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "SingletonLongs";
        }
    }

    private static class Longs extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        Longs(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.LongBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.longsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.LongBuilder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < this.docID) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            read(docId, (LongBuilder) builder);
        }

        private void read(int doc, LongBuilder builder) throws IOException {
            this.docID = doc;
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendLong(numericDocValues.nextValue());
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendLong(numericDocValues.nextValue());
            }
            builder.endPositionEntry();
        }

        @Override
        public int docID() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "Longs";
        }
    }

    private static class SingletonInts extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonInts(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public IntBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.intsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.IntBuilder builder = builder(factory, docs.count())) {
                int lastDoc = -1;
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendInt(Math.toIntExact(numericDocValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            IntBuilder blockBuilder = (IntBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendInt(Math.toIntExact(numericDocValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "SingletonInts";
        }
    }

    private static class Ints extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        Ints(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public IntBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.intsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.IntBuilder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < this.docID) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            read(docId, (IntBuilder) builder);
        }

        private void read(int doc, IntBuilder builder) throws IOException {
            this.docID = doc;
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendInt(Math.toIntExact(numericDocValues.nextValue()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendInt(Math.toIntExact(numericDocValues.nextValue()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docID() {
            // There is a .docID on on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "Ints";
        }
    }

    private static class SingletonDoubles extends BlockDocValuesReader {
        private final NumericDocValues docValues;
        private final ToDouble toDouble;
        private int docID = -1;

        SingletonDoubles(NumericDocValues docValues, ToDouble toDouble) {
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public DoubleBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.doublesFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.DoubleBuilder builder = builder(factory, docs.count())) {
                int lastDoc = -1;
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (docValues.advanceExact(doc)) {
                        builder.appendDouble(toDouble.convert(docValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                    this.docID = doc;
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            this.docID = docId;
            DoubleBuilder blockBuilder = (DoubleBuilder) builder;
            if (docValues.advanceExact(this.docID)) {
                blockBuilder.appendDouble(toDouble.convert(docValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public String toString() {
            return "SingletonDoubles";
        }
    }

    private static class Doubles extends BlockDocValuesReader {
        private final SortedNumericDocValues docValues;
        private final ToDouble toDouble;
        private int docID = -1;

        Doubles(SortedNumericDocValues docValues, ToDouble toDouble) {
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public DoubleBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.doublesFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.DoubleBuilder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < this.docID) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            read(docId, (DoubleBuilder) builder);
        }

        private void read(int doc, DoubleBuilder builder) throws IOException {
            this.docID = doc;
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValueCount();
            if (count == 1) {
                builder.appendDouble(toDouble.convert(docValues.nextValue()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendDouble(toDouble.convert(docValues.nextValue()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public String toString() {
            return "Doubles";
        }
    }

    private static class SingletonOrdinals extends BlockDocValuesReader {
        private final SortedDocValues ordinals;

        SingletonOrdinals(SortedDocValues ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public BytesRefBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.bytesRefsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.SingletonOrdinalsBuilder builder = factory.singletonOrdinalsBuilder(ordinals, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < ordinals.docID()) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (ordinals.advanceExact(doc)) {
                        builder.appendOrd(ordinals.ordValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int doc, Builder builder) throws IOException {
            if (ordinals.advanceExact(doc)) {
                ((BytesRefBuilder) builder).appendBytesRef(ordinals.lookupOrd(ordinals.ordValue()));
            } else {
                builder.appendNull();
            }
        }

        @Override
        public int docID() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "SingletonOrdinals";
        }
    }

    private static class Ordinals extends BlockDocValuesReader {
        private final SortedSetDocValues ordinals;

        Ordinals(SortedSetDocValues ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public BytesRefBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.bytesRefsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BytesRefBuilder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < ordinals.docID()) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int doc, Builder builder) throws IOException {
            read(doc, (BytesRefBuilder) builder);
        }

        private void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == ordinals.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = ordinals.docValueCount();
            if (count == 1) {
                builder.appendBytesRef(ordinals.lookupOrd(ordinals.nextOrd()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBytesRef(ordinals.lookupOrd(ordinals.nextOrd()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docID() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "Ordinals";
        }
    }

    private static class Bytes extends BlockDocValuesReader {
        private final SortedBinaryDocValues docValues;
        private int docID = -1;

        Bytes(SortedBinaryDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public BytesRefBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.bytesRefsFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.BytesRefBuilder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < docID) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        private void read(int doc, BytesRefBuilder builder) throws IOException {
            this.docID = doc;
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValueCount();
            if (count == 1) {
                // TODO read ords in ascending order. Buffers and stuff.
                builder.appendBytesRef(docValues.nextValue());
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBytesRef(docValues.nextValue());
            }
            builder.endPositionEntry();
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public String toString() {
            return "Bytes";
        }
    }

    private static class SingletonBooleans extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonBooleans(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BooleanBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.booleansFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.BooleanBuilder builder = builder(factory, docs.count())) {
                int lastDoc = -1;
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendBoolean(numericDocValues.longValue() != 0);
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            BooleanBuilder blockBuilder = (BooleanBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendBoolean(numericDocValues.longValue() != 0);
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "SingletonBooleans";
        }
    }

    private static class Booleans extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        Booleans(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BooleanBuilder builder(BuilderFactory factory, int expectedCount) {
            return factory.booleansFromDocValues(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.BooleanBuilder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < this.docID) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) throws IOException {
            read(docId, (BooleanBuilder) builder);
        }

        private void read(int doc, BooleanBuilder builder) throws IOException {
            this.docID = doc;
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendBoolean(numericDocValues.nextValue() != 0);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBoolean(numericDocValues.nextValue() != 0);
            }
            builder.endPositionEntry();
        }

        @Override
        public int docID() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "Booleans";
        }
    }

    private static class Nulls extends BlockDocValuesReader {
        private int docID = -1;

        @Override
        public BlockLoader.Builder builder(BuilderFactory factory, int expectedCount) {
            return factory.nulls(expectedCount);
        }

        @Override
        public BlockLoader.Block readValues(BuilderFactory factory, Docs docs) throws IOException {
            try (BlockLoader.Builder builder = builder(factory, docs.count())) {
                for (int i = 0; i < docs.count(); i++) {
                    builder.appendNull();
                }
                return builder.build();
            }
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Builder builder) {
            this.docID = docId;
            builder.appendNull();
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public String toString() {
            return "Nulls";
        }
    }

    /**
     * Convert a {@link String} into a utf-8 {@link BytesRef}.
     */
    protected static BytesRef toBytesRef(BytesRef scratch, String v) {
        int len = UnicodeUtil.maxUTF8Length(v.length());
        if (scratch.bytes.length < len) {
            scratch.bytes = new byte[len];
        }
        scratch.length = UnicodeUtil.UTF16toUTF8(v, 0, v.length(), scratch.bytes);
        return scratch;
    }
}
