/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BlockLoader.BooleanBuilder;
import org.elasticsearch.index.mapper.BlockLoader.Builder;
import org.elasticsearch.index.mapper.BlockLoader.BlockFactory;
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
    protected final Thread creationThread;

    public BlockDocValuesReader() {
        this.creationThread = Thread.currentThread();
    }

    /**
     * Returns the current doc that this reader is on.
     */
    public abstract int docID();

    /**
     * Reads the values of the given documents specified in the input block
     */
    public abstract BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException;

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

    @Override
    public abstract String toString();

    public abstract static class DocValuesBlockLoader implements BlockLoader {
        @Override
        public final Method method() {
            return Method.DOC_VALUES;
        }

        @Override
        public final Block constant(BlockFactory factory, int size) {
            throw new UnsupportedOperationException();
        }
    }

    public static class LongsBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;

        public LongsBlockLoader(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.longs(expectedCount);
        }

        @Override
        public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonLongs(singleton);
            }
            return new Longs(docValues);
        }
    }

    private static class SingletonLongs extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonLongs(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.LongBuilder builder = factory.longsFromDocValues(docs.count())) {
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
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.LongBuilder builder = factory.longsFromDocValues(docs.count())) {
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

    public static class IntsBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;

        public IntsBlockLoader(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.ints(expectedCount);
        }

        @Override
        public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonInts(singleton);
            }
            return new Ints(docValues);
        }
    }

    private static class SingletonInts extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonInts(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.IntBuilder builder = factory.intsFromDocValues(docs.count())) {
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
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.IntBuilder builder = factory.intsFromDocValues(docs.count())) {
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

    /**
     * Convert from the stored {@link long} into the {@link double} to load.
     * Sadly, this will go megamorphic pretty quickly and slow us down,
     * but it gets the job done for now.
     */
    public interface ToDouble {
        double convert(long v);
    }

    public static class DoublesBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;
        private final ToDouble toDouble;

        public DoublesBlockLoader(String fieldName, ToDouble toDouble) {
            this.fieldName = fieldName;
            this.toDouble = toDouble;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.doubles(expectedCount);
        }

        @Override
        public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonDoubles(singleton, toDouble);
            }
            return new Doubles(docValues, toDouble);
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
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.DoubleBuilder builder = factory.doublesFromDocValues(docs.count())) {
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
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.DoubleBuilder builder = factory.doublesFromDocValues(docs.count())) {
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

    public static class BytesRefsFromOrdsBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;

        public BytesRefsFromOrdsBlockLoader(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
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
    }

    private static class SingletonOrdinals extends BlockDocValuesReader {
        private final SortedDocValues ordinals;

        SingletonOrdinals(SortedDocValues ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
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
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BytesRefBuilder builder = factory.bytesRefsFromDocValues(docs.count())) {
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

    public static class BytesRefsFromBinaryBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;

        public BytesRefsFromBinaryBlockLoader(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
            BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
            if (docValues == null) {
                // NOCOMMIT should we just there's got to be a constant null method?
                return new Nulls();
            }
            return new BytesRefsFromBinary(docValues);
        }
    }

    private static class BytesRefsFromBinary extends BlockDocValuesReader {
        private final BinaryDocValues docValues;
        private final ByteArrayStreamInput in = new ByteArrayStreamInput();
        private final BytesRef scratch = new BytesRef();

        private int docID = -1;

        BytesRefsFromBinary(BinaryDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count())) {
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
            BytesRef bytes = docValues.binaryValue();
            assert bytes.length > 0;
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            int count = in.readVInt();
            scratch.bytes = bytes.bytes;

            if (count == 1) {
                scratch.length = in.readVInt();
                scratch.offset = in.getPosition();
                builder.appendBytesRef(scratch);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                scratch.length = in.readVInt();
                scratch.offset = in.getPosition();
                in.setPosition(scratch.offset + scratch.length);
                builder.appendBytesRef(scratch);
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

    public static class BooleansBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;

        public BooleansBlockLoader(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public BooleanBuilder builder(BlockFactory factory, int expectedCount) {
            return factory.booleans(expectedCount);
        }

        @Override
        public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonBooleans(singleton);
            }
            return new Booleans(docValues);
        }
    }

    private static class SingletonBooleans extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonBooleans(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.BooleanBuilder builder = factory.booleansFromDocValues(docs.count())) {
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
        public BlockLoader.Block readValues(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.BooleanBuilder builder = factory.booleansFromDocValues(docs.count())) {
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
        public BlockLoader.Block readValues(BlockLoader.BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.Builder builder = factory.nulls(docs.count())) {
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
