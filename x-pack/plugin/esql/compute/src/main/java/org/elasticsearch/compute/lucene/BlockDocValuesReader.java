/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

import static org.elasticsearch.compute.lucene.ValueSources.checkMultiValue;

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
     * The {@link Block.Builder} for data of this type.
     */
    public abstract Block.Builder builder(int positionCount);

    /**
     * Reads the values of the given documents specified in the input block
     */
    public abstract Block readValues(IntVector docs) throws IOException;

    /**
     * Reads the values of the given document into the builder
     */
    public abstract void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException;

    /**
     * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
     */
    public static boolean canReuse(BlockDocValuesReader reader, int startingDocID) {
        return reader != null && reader.creationThread == Thread.currentThread() && reader.docID() <= startingDocID;
    }

    public static BlockDocValuesReader createBlockReader(
        ValuesSource valuesSource,
        ValuesSourceType valuesSourceType,
        ElementType elementType,
        LeafReaderContext leafReaderContext
    ) throws IOException {
        if (CoreValuesSourceType.NUMERIC.equals(valuesSourceType) || CoreValuesSourceType.DATE.equals(valuesSourceType)) {
            ValuesSource.Numeric numericVS = (ValuesSource.Numeric) valuesSource;
            if (numericVS.isFloatingPoint()) {
                if (elementType != ElementType.DOUBLE) {
                    throw new UnsupportedOperationException("can't extract [" + elementType + "] from floating point fields");
                }
                final SortedNumericDoubleValues doubleValues = numericVS.doubleValues(leafReaderContext);
                final NumericDoubleValues singleton = FieldData.unwrapSingleton(doubleValues);
                if (singleton != null) {
                    return new DoubleSingletonValuesReader(singleton);
                }
                return new DoubleValuesReader(doubleValues);
            } else {
                final SortedNumericDocValues longValues = numericVS.longValues(leafReaderContext);
                final NumericDocValues singleton = DocValues.unwrapSingleton(longValues);
                if (singleton != null) {
                    return switch (elementType) {
                        case LONG -> new LongSingletonValuesReader(singleton);
                        case INT -> new IntSingletonValuesReader(singleton);
                        default -> throw new UnsupportedOperationException("can't extract [" + elementType + "] from integer fields");
                    };
                }
                return switch (elementType) {
                    case LONG -> new LongValuesReader(longValues);
                    case INT -> new IntValuesReader(longValues);
                    default -> throw new UnsupportedOperationException("can't extract [" + elementType + "] from integer fields");
                };
            }
        }
        if (CoreValuesSourceType.KEYWORD.equals(valuesSourceType)) {
            if (elementType != ElementType.BYTES_REF) {
                throw new UnsupportedOperationException("can't extract [" + elementType + "] from keywords");
            }
            final ValuesSource.Bytes bytesVS = (ValuesSource.Bytes) valuesSource;
            final SortedBinaryDocValues bytesValues = bytesVS.bytesValues(leafReaderContext);
            return new BytesValuesReader(bytesValues);
        }
        if (CoreValuesSourceType.BOOLEAN.equals(valuesSourceType)) {
            if (elementType != ElementType.BOOLEAN) {
                throw new UnsupportedOperationException("can't extract [" + elementType + "] from booleans");
            }
            ValuesSource.Numeric numericVS = (ValuesSource.Numeric) valuesSource;
            final SortedNumericDocValues longValues = numericVS.longValues(leafReaderContext);
            final NumericDocValues singleton = DocValues.unwrapSingleton(longValues);
            if (singleton != null) {
                return new BooleanSingletonValuesReader(singleton);
            }
            return new BooleanValuesReader(longValues);
        }
        throw new IllegalArgumentException("Field type [" + valuesSourceType.typeName() + "] is not supported");
    }

    private static class LongSingletonValuesReader extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        LongSingletonValuesReader(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public LongBlock.Builder builder(int positionCount) {
            return LongBlock.newBlockBuilder(positionCount);
        }

        @Override
        public LongBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    blockBuilder.appendLong(numericDocValues.longValue());
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            LongBlock.Builder blockBuilder = (LongBlock.Builder) builder;
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
            return "LongSingletonValuesReader";
        }
    }

    private static class LongValuesReader extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        LongValuesReader(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public LongBlock.Builder builder(int positionCount) {
            return LongBlock.newBlockBuilder(positionCount);
        }

        @Override
        public LongBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = LongBlock.newBlockBuilder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    checkMultiValue(doc, numericDocValues.docValueCount());
                    blockBuilder.appendLong(numericDocValues.nextValue());
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
                this.docID = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            this.docID = docId;
            LongBlock.Builder blockBuilder = (LongBlock.Builder) builder;
            if (numericDocValues.advanceExact(docId)) {
                checkMultiValue(docId, numericDocValues.docValueCount());
                blockBuilder.appendLong(numericDocValues.nextValue());
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "LongValuesReader";
        }
    }

    private static class IntSingletonValuesReader extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        IntSingletonValuesReader(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public IntBlock.Builder builder(int positionCount) {
            return IntBlock.newBlockBuilder(positionCount);
        }

        @Override
        public IntBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    blockBuilder.appendInt(Math.toIntExact(numericDocValues.longValue()));
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            IntBlock.Builder blockBuilder = (IntBlock.Builder) builder;
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
            return "LongSingletonValuesReader";
        }
    }

    private static class IntValuesReader extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        IntValuesReader(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public IntBlock.Builder builder(int positionCount) {
            return IntBlock.newBlockBuilder(positionCount);
        }

        @Override
        public IntBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    // TODO this may not be true after sorting many docs in a single segment.
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    checkMultiValue(doc, numericDocValues.docValueCount());
                    blockBuilder.appendInt(Math.toIntExact(numericDocValues.nextValue()));
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
                this.docID = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            this.docID = docId;
            IntBlock.Builder blockBuilder = (IntBlock.Builder) builder;
            if (numericDocValues.advanceExact(docId)) {
                checkMultiValue(docId, numericDocValues.docValueCount());
                blockBuilder.appendInt(Math.toIntExact(numericDocValues.nextValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            // There is a .docID on on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return "LongValuesReader";
        }
    }

    private static class DoubleSingletonValuesReader extends BlockDocValuesReader {
        private final NumericDoubleValues numericDocValues;
        private int docID = -1;

        DoubleSingletonValuesReader(NumericDoubleValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public DoubleBlock.Builder builder(int positionCount) {
            return DoubleBlock.newBlockBuilder(positionCount);
        }

        @Override
        public DoubleBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    blockBuilder.appendDouble(numericDocValues.doubleValue());
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
                this.docID = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            this.docID = docId;
            DoubleBlock.Builder blockBuilder = (DoubleBlock.Builder) builder;
            if (numericDocValues.advanceExact(this.docID)) {
                blockBuilder.appendDouble(numericDocValues.doubleValue());
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
            return "DoubleSingletonValuesReader";
        }
    }

    private static class DoubleValuesReader extends BlockDocValuesReader {
        private final SortedNumericDoubleValues numericDocValues;
        private int docID = -1;

        DoubleValuesReader(SortedNumericDoubleValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public DoubleBlock.Builder builder(int positionCount) {
            return DoubleBlock.newBlockBuilder(positionCount);
        }

        @Override
        public DoubleBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = DoubleBlock.newBlockBuilder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    checkMultiValue(doc, numericDocValues.docValueCount());
                    blockBuilder.appendDouble(numericDocValues.nextValue());
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
                this.docID = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            this.docID = docId;
            DoubleBlock.Builder blockBuilder = (DoubleBlock.Builder) builder;
            if (numericDocValues.advanceExact(this.docID)) {
                checkMultiValue(this.docID, numericDocValues.docValueCount());
                blockBuilder.appendDouble(numericDocValues.nextValue());
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
            return "DoubleValuesReader";
        }
    }

    private static class BytesValuesReader extends BlockDocValuesReader {
        private final SortedBinaryDocValues binaryDV;
        private int docID = -1;

        BytesValuesReader(SortedBinaryDocValues binaryDV) {
            this.binaryDV = binaryDV;
        }

        @Override
        public BytesRefBlock.Builder builder(int positionCount) {
            return BytesRefBlock.newBlockBuilder(positionCount);
        }

        @Override
        public BytesRefBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < docs.getPositionCount(); i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (binaryDV.advanceExact(doc)) {
                    checkMultiValue(doc, binaryDV.docValueCount());
                    blockBuilder.appendBytesRef(binaryDV.nextValue());
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
                this.docID = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            this.docID = docId;
            BytesRefBlock.Builder blockBuilder = (BytesRefBlock.Builder) builder;
            if (binaryDV.advanceExact(this.docID)) {
                checkMultiValue(this.docID, binaryDV.docValueCount());
                blockBuilder.appendBytesRef(binaryDV.nextValue());
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
            return "BytesValuesReader";
        }
    }

    private static class BooleanSingletonValuesReader extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        BooleanSingletonValuesReader(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BooleanBlock.Builder builder(int positionCount) {
            return BooleanBlock.newBlockBuilder(positionCount);
        }

        @Override
        public BooleanBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    blockBuilder.appendBoolean(numericDocValues.longValue() != 0);
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            BooleanBlock.Builder blockBuilder = (BooleanBlock.Builder) builder;
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
            return getClass().getSimpleName();
        }
    }

    private static class BooleanValuesReader extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private int docID = -1;

        BooleanValuesReader(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BooleanBlock.Builder builder(int positionCount) {
            return BooleanBlock.newBlockBuilder(positionCount);
        }

        @Override
        public BooleanBlock readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = builder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < positionCount; i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (numericDocValues.advanceExact(doc)) {
                    checkMultiValue(doc, numericDocValues.docValueCount());
                    blockBuilder.appendBoolean(numericDocValues.nextValue() != 0);
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
                this.docID = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public void readValuesFromSingleDoc(int docId, Block.Builder builder) throws IOException {
            this.docID = docId;
            BooleanBlock.Builder blockBuilder = (BooleanBlock.Builder) builder;
            if (numericDocValues.advanceExact(this.docID)) {
                blockBuilder.appendBoolean(numericDocValues.nextValue() != 0);
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docID() {
            // There is a .docID on the numericDocValues but it is often not implemented.
            return docID;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }
}
