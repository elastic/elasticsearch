/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader.BlockFactory;
import org.elasticsearch.index.mapper.BlockLoader.BooleanBuilder;
import org.elasticsearch.index.mapper.BlockLoader.Builder;
import org.elasticsearch.index.mapper.BlockLoader.BytesRefBuilder;
import org.elasticsearch.index.mapper.BlockLoader.Docs;
import org.elasticsearch.index.mapper.BlockLoader.DoubleBuilder;
import org.elasticsearch.index.mapper.BlockLoader.IntBuilder;
import org.elasticsearch.index.mapper.BlockLoader.LongBuilder;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * A reader that supports reading doc-values from a Lucene segment in Block fashion.
 */
public abstract class BlockDocValuesReader implements BlockLoader.AllReader {
    private final Thread creationThread;

    public BlockDocValuesReader() {
        this.creationThread = Thread.currentThread();
    }

    protected abstract int docId();

    /**
     * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
     */
    @Override
    public final boolean canReuse(int startingDocID) {
        return creationThread == Thread.currentThread() && docId() <= startingDocID;
    }

    @Override
    public abstract String toString();

    public abstract static class DocValuesBlockLoader implements BlockLoader {
        public abstract AllReader reader(LeafReaderContext context) throws IOException;

        @Override
        public final ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            return reader(context);
        }

        @Override
        public final RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            return reader(context);
        }

        @Override
        public final StoredFieldsSpec rowStrideStoredFieldSpec() {
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }

        @Override
        public boolean supportsOrdinals() {
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
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
        public AllReader reader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return new SingletonLongs(singleton);
                }
                return new Longs(docValues);
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                return new SingletonLongs(singleton);
            }
            return new ConstantNullsReader();
        }
    }

    private static class SingletonLongs extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonLongs(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            BlockLoader.LongBuilder blockBuilder = (BlockLoader.LongBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendLong(numericDocValues.longValue());
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonLongs";
        }
    }

    private static class Longs extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        Longs(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
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
        public int docId() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Longs";
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
        public AllReader reader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return new SingletonInts(singleton);
                }
                return new Ints(docValues);
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                return new SingletonInts(singleton);
            }
            return new ConstantNullsReader();
        }
    }

    private static class SingletonInts extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonInts(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            IntBuilder blockBuilder = (IntBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendInt(Math.toIntExact(numericDocValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonInts";
        }
    }

    private static class Ints extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        Ints(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
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
        public int docId() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Ints";
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
        public AllReader reader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return new SingletonDoubles(singleton, toDouble);
                }
                return new Doubles(docValues, toDouble);
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                return new SingletonDoubles(singleton, toDouble);
            }
            return new ConstantNullsReader();
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
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            this.docID = docId;
            DoubleBuilder blockBuilder = (DoubleBuilder) builder;
            if (docValues.advanceExact(this.docID)) {
                blockBuilder.appendDouble(toDouble.convert(docValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return docID;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonDoubles";
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
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
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
        public int docId() {
            return docID;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Doubles";
        }
    }

    public static class DenseVectorBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;
        private final int dimensions;

        public DenseVectorBlockLoader(String fieldName, int dimensions) {
            this.fieldName = fieldName;
            this.dimensions = dimensions;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.denseVectors(expectedCount, dimensions);
        }

        @Override
        public AllReader reader(LeafReaderContext context) throws IOException {
            FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(fieldName);
            if (floatVectorValues != null) {
                return new DenseVectorValuesBlockReader(floatVectorValues, dimensions);
            }
            return new ConstantNullsReader();
        }
    }

    private static class DenseVectorValuesBlockReader extends BlockDocValuesReader {
        private final FloatVectorValues floatVectorValues;
        private final int dimensions;

        DenseVectorValuesBlockReader(FloatVectorValues floatVectorValues, int dimensions) {
            this.floatVectorValues = floatVectorValues;
            this.dimensions = dimensions;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
            // Doubles from doc values ensures that the values are in order
            try (BlockLoader.FloatBuilder builder = factory.denseVectors(docs.count(), dimensions)) {
                for (int i = 0; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < floatVectorValues.docID()) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.FloatBuilder) builder);
        }

        private void read(int doc, BlockLoader.FloatBuilder builder) throws IOException {
            if (floatVectorValues.advance(doc) == doc) {
                builder.beginPositionEntry();
                float[] floats = floatVectorValues.vectorValue();
                assert floats.length == dimensions
                    : "unexpected dimensions for vector value; expected " + dimensions + " but got " + floats.length;
                for (float aFloat : floats) {
                    builder.appendFloat(aFloat);
                }
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }

        @Override
        public int docId() {
            return floatVectorValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatVectorValuesBlockReader";
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
        public AllReader reader(LeafReaderContext context) throws IOException {
            SortedSetDocValues docValues = context.reader().getSortedSetDocValues(fieldName);
            if (docValues != null) {
                SortedDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return new SingletonOrdinals(singleton);
                }
                return new Ordinals(docValues);
            }
            SortedDocValues singleton = context.reader().getSortedDocValues(fieldName);
            if (singleton != null) {
                return new SingletonOrdinals(singleton);
            }
            return new ConstantNullsReader();
        }

        @Override
        public boolean supportsOrdinals() {
            return true;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
            return DocValues.getSortedSet(context.reader(), fieldName);
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds[" + fieldName + "]";
        }
    }

    private static class SingletonOrdinals extends BlockDocValuesReader {
        private final SortedDocValues ordinals;

        SingletonOrdinals(SortedDocValues ordinals) {
            this.ordinals = ordinals;
        }

        private BlockLoader.Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.advanceExact(docId)) {
                BytesRef v = ordinals.lookupOrd(ordinals.ordValue());
                // the returned BytesRef can be reused
                return factory.constantBytes(BytesRef.deepCopyOf(v));
            } else {
                return factory.constantNulls();
            }
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
            if (docs.count() == 1) {
                return readSingleDoc(factory, docs.get(0));
            }
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            if (ordinals.advanceExact(docId)) {
                ((BytesRefBuilder) builder).appendBytesRef(ordinals.lookupOrd(ordinals.ordValue()));
            } else {
                builder.appendNull();
            }
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonOrdinals";
        }
    }

    private static class Ordinals extends BlockDocValuesReader {
        private final SortedSetDocValues ordinals;

        Ordinals(SortedSetDocValues ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == ordinals.advanceExact(docId)) {
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
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Ordinals";
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
        public AllReader reader(LeafReaderContext context) throws IOException {
            BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
            if (docValues == null) {
                return new ConstantNullsReader();
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
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
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
        public int docId() {
            return docID;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Bytes";
        }
    }

    public static class DenseVectorFromBinaryBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;
        private final int dims;
        private final IndexVersion indexVersion;

        public DenseVectorFromBinaryBlockLoader(String fieldName, int dims, IndexVersion indexVersion) {
            this.fieldName = fieldName;
            this.dims = dims;
            this.indexVersion = indexVersion;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.denseVectors(expectedCount, dims);
        }

        @Override
        public AllReader reader(LeafReaderContext context) throws IOException {
            BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
            if (docValues == null) {
                return new ConstantNullsReader();
            }
            return new DenseVectorFromBinary(docValues, dims, indexVersion);
        }
    }

    private static class DenseVectorFromBinary extends BlockDocValuesReader {
        private final BinaryDocValues docValues;
        private final IndexVersion indexVersion;
        private final int dimensions;
        private final float[] scratch;

        private int docID = -1;

        DenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            this.docValues = docValues;
            this.scratch = new float[dims];
            this.indexVersion = indexVersion;
            this.dimensions = dims;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
            try (BlockLoader.FloatBuilder builder = factory.denseVectors(docs.count(), dimensions)) {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.FloatBuilder) builder);
        }

        private void read(int doc, BlockLoader.FloatBuilder builder) throws IOException {
            this.docID = doc;
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytesRef = docValues.binaryValue();
            assert bytesRef.length > 0;
            VectorEncoderDecoder.decodeDenseVector(indexVersion, bytesRef, scratch);

            builder.beginPositionEntry();
            for (float value : scratch) {
                builder.appendFloat(value);
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return docID;
        }

        @Override
        public String toString() {
            return "DenseVectorFromBinary.Bytes";
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
        public AllReader reader(LeafReaderContext context) throws IOException {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return new SingletonBooleans(singleton);
                }
                return new Booleans(docValues);
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                return new SingletonBooleans(singleton);
            }
            return new ConstantNullsReader();
        }
    }

    private static class SingletonBooleans extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonBooleans(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            BooleanBuilder blockBuilder = (BooleanBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendBoolean(numericDocValues.longValue() != 0);
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonBooleans";
        }
    }

    private static class Booleans extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        Booleans(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs) throws IOException {
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
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
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
        public int docId() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Booleans";
        }
    }
}
