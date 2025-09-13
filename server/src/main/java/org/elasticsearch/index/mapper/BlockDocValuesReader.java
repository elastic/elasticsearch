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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.COSINE_MAGNITUDE_FIELD_SUFFIX;

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

    // Used for testing.
    interface NumericDocValuesAccessor {
        NumericDocValues numericDocValues();
    }

    static class SingletonLongs extends BlockDocValuesReader implements NumericDocValuesAccessor {
        final NumericDocValues numericDocValues;

        SingletonLongs(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (numericDocValues instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block result = direct.tryRead(factory, docs, offset, nullsFiltered, null);
                if (result != null) {
                    return result;
                }
            }
            try (BlockLoader.LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendLong(numericDocValues.longValue());
                    } else {
                        builder.appendNull();
                    }
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

        @Override
        public NumericDocValues numericDocValues() {
            return numericDocValues;
        }
    }

    static class Longs extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;

        Longs(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
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
            return numericDocValues.docID();
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
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendInt(Math.toIntExact(numericDocValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
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

        Ints(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
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
            return numericDocValues.docID();
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

    static class SingletonDoubles extends BlockDocValuesReader implements NumericDocValuesAccessor {
        private final NumericDocValues docValues;
        private final ToDouble toDouble;

        SingletonDoubles(NumericDocValues docValues, ToDouble toDouble) {
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block result = direct.tryRead(factory, docs, offset, nullsFiltered, toDouble);
                if (result != null) {
                    return result;
                }
            }
            try (BlockLoader.DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (docValues.advanceExact(doc)) {
                        builder.appendDouble(toDouble.convert(docValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            DoubleBuilder blockBuilder = (DoubleBuilder) builder;
            if (docValues.advanceExact(docId)) {
                blockBuilder.appendDouble(toDouble.convert(docValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonDoubles";
        }

        @Override
        public NumericDocValues numericDocValues() {
            return docValues;
        }
    }

    static class Doubles extends BlockDocValuesReader {
        private final SortedNumericDocValues docValues;
        private final ToDouble toDouble;

        Doubles(SortedNumericDocValues docValues, ToDouble toDouble) {
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
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
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Doubles";
        }
    }

    public static class DenseVectorBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;
        private final int dimensions;
        private final DenseVectorFieldMapper.DenseVectorFieldType fieldType;

        public DenseVectorBlockLoader(String fieldName, int dimensions, DenseVectorFieldMapper.DenseVectorFieldType fieldType) {
            this.fieldName = fieldName;
            this.dimensions = dimensions;
            this.fieldType = fieldType;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.denseVectors(expectedCount, dimensions);
        }

        @Override
        public AllReader reader(LeafReaderContext context) throws IOException {
            switch (fieldType.getElementType()) {
                case FLOAT -> {
                    FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(fieldName);
                    if (floatVectorValues != null) {
                        if (fieldType.isNormalized()) {
                            NumericDocValues magnitudeDocValues = context.reader()
                                .getNumericDocValues(fieldType.name() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                            return new FloatDenseVectorNormalizedValuesBlockReader(floatVectorValues, dimensions, magnitudeDocValues);
                        }
                        return new FloatDenseVectorValuesBlockReader(floatVectorValues, dimensions);
                    }
                }
                case BYTE -> {
                    ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                    if (byteVectorValues != null) {
                        return new ByteDenseVectorValuesBlockReader(byteVectorValues, dimensions);
                    }
                }
                case BIT -> {
                    ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                    if (byteVectorValues != null) {
                        return new BitDenseVectorValuesBlockReader(byteVectorValues, dimensions);
                    }
                }
            }

            return new ConstantNullsReader();
        }
    }

    private abstract static class DenseVectorValuesBlockReader<T extends KnnVectorValues> extends BlockDocValuesReader {

        protected final T vectorValues;
        protected final KnnVectorValues.DocIndexIterator iterator;
        protected final int dimensions;

        DenseVectorValuesBlockReader(T vectorValues, int dimensions) {
            this.vectorValues = vectorValues;
            iterator = vectorValues.iterator();
            this.dimensions = dimensions;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            // Doubles from doc values ensures that the values are in order
            try (BlockLoader.FloatBuilder builder = factory.denseVectors(docs.count() - offset, dimensions)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.FloatBuilder) builder);
        }

        private void read(int doc, BlockLoader.FloatBuilder builder) throws IOException {
            assertDimensions();

            if (iterator.docID() > doc) {
                builder.appendNull();
            } else if (iterator.docID() == doc || iterator.advance(doc) == doc) {
                builder.beginPositionEntry();
                appendDoc(builder);
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }

        protected abstract void appendDoc(BlockLoader.FloatBuilder builder) throws IOException;

        @Override
        public int docId() {
            return iterator.docID();
        }

        protected void assertDimensions() {
            assert vectorValues.dimension() == dimensions
                : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension();
        }
    }

    private static class FloatDenseVectorValuesBlockReader extends DenseVectorValuesBlockReader<FloatVectorValues> {

        FloatDenseVectorValuesBlockReader(FloatVectorValues floatVectorValues, int dimensions) {
            super(floatVectorValues, dimensions);
        }

        protected void appendDoc(BlockLoader.FloatBuilder builder) throws IOException {
            float[] floats = vectorValues.vectorValue(iterator.index());
            for (float aFloat : floats) {
                builder.appendFloat(aFloat);
            }
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatDenseVectorValuesBlockReader";
        }
    }

    private static class FloatDenseVectorNormalizedValuesBlockReader extends DenseVectorValuesBlockReader<FloatVectorValues> {
        private final NumericDocValues magnitudeDocValues;

        FloatDenseVectorNormalizedValuesBlockReader(
            FloatVectorValues floatVectorValues,
            int dimensions,
            NumericDocValues magnitudeDocValues
        ) {
            super(floatVectorValues, dimensions);
            this.magnitudeDocValues = magnitudeDocValues;
        }

        @Override
        protected void appendDoc(BlockLoader.FloatBuilder builder) throws IOException {
            float magnitude = 1.0f;
            // If all vectors are normalized, no doc values will be present. The vector may be normalized already, so we may not have a
            // stored magnitude for all docs
            if ((magnitudeDocValues != null) && magnitudeDocValues.advanceExact(iterator.docID())) {
                magnitude = Float.intBitsToFloat((int) magnitudeDocValues.longValue());
            }
            float[] floats = vectorValues.vectorValue(iterator.index());
            for (float aFloat : floats) {
                builder.appendFloat(aFloat * magnitude);
            }
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatDenseVectorNormalizedValuesBlockReader";
        }
    }

    private static class ByteDenseVectorValuesBlockReader extends DenseVectorValuesBlockReader<ByteVectorValues> {
        ByteDenseVectorValuesBlockReader(ByteVectorValues floatVectorValues, int dimensions) {
            super(floatVectorValues, dimensions);
        }

        protected void appendDoc(BlockLoader.FloatBuilder builder) throws IOException {
            byte[] bytes = vectorValues.vectorValue(iterator.index());
            for (byte aFloat : bytes) {
                builder.appendFloat(aFloat);
            }
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.ByteDenseVectorValuesBlockReader";
        }
    }

    private static class BitDenseVectorValuesBlockReader extends ByteDenseVectorValuesBlockReader {

        BitDenseVectorValuesBlockReader(ByteVectorValues floatVectorValues, int dimensions) {
            super(floatVectorValues, dimensions);
        }

        @Override
        protected void assertDimensions() {
            assert vectorValues.dimension() * Byte.SIZE == dimensions
                : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension() * Byte.SIZE;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.BitDenseVectorValuesBlockReader";
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
                return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
            } else {
                return factory.constantNulls(1);
            }
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }
            if (ordinals instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block block = direct.tryRead(factory, docs, offset, nullsFiltered, null);
                if (block != null) {
                    return block;
                }
            }
            try (var builder = factory.singletonOrdinalsBuilder(ordinals, docs.count() - offset, false)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
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
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }
            try (var builder = factory.sortedSetOrdinalsBuilder(ordinals, docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < ordinals.docID()) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (ordinals.advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    int count = ordinals.docValueCount();
                    if (count == 1) {
                        builder.appendOrd(Math.toIntExact(ordinals.nextOrd()));
                    } else {
                        builder.beginPositionEntry();
                        for (int c = 0; c < count; c++) {
                            builder.appendOrd(Math.toIntExact(ordinals.nextOrd()));
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        private BlockLoader.Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            int count = ordinals.docValueCount();
            if (count == 1) {
                BytesRef v = ordinals.lookupOrd(ordinals.nextOrd());
                return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
            }
            try (var builder = factory.bytesRefsFromDocValues(count)) {
                builder.beginPositionEntry();
                for (int c = 0; c < count; c++) {
                    BytesRef v = ordinals.lookupOrd(ordinals.nextOrd());
                    builder.appendBytesRef(v);
                }
                builder.endPositionEntry();
                return builder.build();
            }
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

    public static class BytesRefsFromCustomBinaryBlockLoader extends DocValuesBlockLoader {
        private final String fieldName;

        public BytesRefsFromCustomBinaryBlockLoader(String fieldName) {
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
            return new BytesRefsFromCustomBinary(docValues);
        }
    }

    abstract static class AbstractBytesRefsFromBinary extends BlockDocValuesReader {
        protected final BinaryDocValues docValues;

        AbstractBytesRefsFromBinary(BinaryDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        abstract void read(int docId, BytesRefBuilder builder) throws IOException;
    }

    /**
     * Read BinaryDocValues encoded by {@link BinaryFieldMapper.CustomBinaryDocValuesField}
     */
    static class BytesRefsFromCustomBinary extends AbstractBytesRefsFromBinary {
        private final ByteArrayStreamInput in = new ByteArrayStreamInput();
        private final BytesRef scratch = new BytesRef();

        BytesRefsFromCustomBinary(BinaryDocValues docValues) {
            super(docValues);
        }

        @Override
        void read(int doc, BytesRefBuilder builder) throws IOException {
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
        public String toString() {
            return "BlockDocValuesReader.BytesCustom";
        }
    }

    /**
     * Read BinaryDocValues with no additional structure in the BytesRefs.
     * Each BytesRef from the doc values maps directly to a value in the block loader.
     */
    public static class BytesRefsFromBinary extends AbstractBytesRefsFromBinary {
        public BytesRefsFromBinary(BinaryDocValues docValues) {
            super(docValues);
        }

        @Override
        void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.binaryValue();
            builder.appendBytesRef(bytes);
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
        private final ElementType elementType;

        public DenseVectorFromBinaryBlockLoader(String fieldName, int dims, IndexVersion indexVersion, ElementType elementType) {
            this.fieldName = fieldName;
            this.dims = dims;
            this.indexVersion = indexVersion;
            this.elementType = elementType;
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
            return switch (elementType) {
                case FLOAT -> new FloatDenseVectorFromBinary(docValues, dims, indexVersion);
                case BYTE -> new ByteDenseVectorFromBinary(docValues, dims, indexVersion);
                case BIT -> new BitDenseVectorFromBinary(docValues, dims, indexVersion);
            };
        }
    }

    // Abstract base for dense vector readers
    private abstract static class AbstractDenseVectorFromBinary<T> extends BlockDocValuesReader {
        protected final BinaryDocValues docValues;
        protected final IndexVersion indexVersion;
        protected final int dimensions;
        protected final T scratch;

        AbstractDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion, T scratch) {
            this.docValues = docValues;
            this.indexVersion = indexVersion;
            this.dimensions = dims;
            this.scratch = scratch;
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.FloatBuilder) builder);
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.FloatBuilder builder = factory.denseVectors(docs.count() - offset, dimensions)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, BlockLoader.FloatBuilder builder) throws IOException {
            if (docValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            BytesRef bytesRef = docValues.binaryValue();
            assert bytesRef.length > 0;
            decodeDenseVector(bytesRef, scratch);

            builder.beginPositionEntry();
            writeScratchToBuilder(scratch, builder);
            builder.endPositionEntry();
        }

        protected abstract void decodeDenseVector(BytesRef bytesRef, T scratch);

        protected abstract void writeScratchToBuilder(T scratch, BlockLoader.FloatBuilder builder);
    }

    private static class FloatDenseVectorFromBinary extends AbstractDenseVectorFromBinary<float[]> {
        FloatDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            super(docValues, dims, indexVersion, new float[dims]);
        }

        @Override
        protected void writeScratchToBuilder(float[] scratch, BlockLoader.FloatBuilder builder) {
            for (float value : scratch) {
                builder.appendFloat(value);
            }
        }

        @Override
        protected void decodeDenseVector(BytesRef bytesRef, float[] scratch) {
            VectorEncoderDecoder.decodeDenseVector(indexVersion, bytesRef, scratch);
        }

        @Override
        public String toString() {
            return "FloatDenseVectorFromBinary.Bytes";
        }
    }

    private static class ByteDenseVectorFromBinary extends AbstractDenseVectorFromBinary<byte[]> {
        ByteDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            this(docValues, dims, indexVersion, dims);
        }

        protected ByteDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion, int readScratchSize) {
            super(docValues, dims, indexVersion, new byte[readScratchSize]);
        }

        @Override
        public String toString() {
            return "ByteDenseVectorFromBinary.Bytes";
        }

        protected void writeScratchToBuilder(byte[] scratch, BlockLoader.FloatBuilder builder) {
            for (byte value : scratch) {
                builder.appendFloat(value);
            }
        }

        protected void decodeDenseVector(BytesRef bytesRef, byte[] scratch) {
            VectorEncoderDecoder.decodeDenseVector(indexVersion, bytesRef, scratch);
        }
    }

    private static class BitDenseVectorFromBinary extends ByteDenseVectorFromBinary {
        BitDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            super(docValues, dims, indexVersion, dims / Byte.SIZE);
        }

        @Override
        public String toString() {
            return "BitDenseVectorFromBinary.Bytes";
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
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
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

        Booleans(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
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
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Booleans";
        }
    }
}
