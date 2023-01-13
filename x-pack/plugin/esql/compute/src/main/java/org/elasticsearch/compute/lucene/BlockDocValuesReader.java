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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
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
    public abstract Block readValues(IntVector docs) throws IOException;

    /**
     * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
     */
    public static boolean canReuse(BlockDocValuesReader reader, int startingDocID) {
        return reader != null && reader.creationThread == Thread.currentThread() && reader.docID() <= startingDocID;
    }

    public static BlockDocValuesReader createBlockReader(
        ValuesSource valuesSource,
        ValuesSourceType valuesSourceType,
        LeafReaderContext leafReaderContext
    ) throws IOException {
        if (CoreValuesSourceType.NUMERIC.equals(valuesSourceType) || CoreValuesSourceType.DATE.equals(valuesSourceType)) {
            ValuesSource.Numeric numericVS = (ValuesSource.Numeric) valuesSource;
            if (numericVS.isFloatingPoint()) {
                final SortedNumericDoubleValues doubleValues = numericVS.doubleValues(leafReaderContext);
                return new DoubleValuesReader(doubleValues);
            } else {
                final SortedNumericDocValues longValues = numericVS.longValues(leafReaderContext);
                return new LongValuesReader(longValues);
            }
        }
        if (CoreValuesSourceType.KEYWORD.equals(valuesSourceType)) {
            final ValuesSource.Bytes bytesVS = (ValuesSource.Bytes) valuesSource;
            final SortedBinaryDocValues bytesValues = bytesVS.bytesValues(leafReaderContext);
            return new BytesValuesReader(bytesValues);
        }
        throw new IllegalArgumentException("Field type [" + valuesSourceType.typeName() + "] is not supported");
    }

    private static class LongValuesReader extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        LongValuesReader(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = DocValues.unwrapSingleton(numericDocValues);
        }

        @Override
        public Block readValues(IntVector docs) throws IOException {
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
                    blockBuilder.appendLong(numericDocValues.longValue());
                } else {
                    blockBuilder.appendNull();
                }
                lastDoc = doc;
            }
            return blockBuilder.build();
        }

        @Override
        public int docID() {
            return numericDocValues.docID();
        }
    }

    private static class DoubleValuesReader extends BlockDocValuesReader {
        private final NumericDoubleValues numericDocValues;
        private int docID = -1;

        DoubleValuesReader(SortedNumericDoubleValues numericDocValues) {
            this.numericDocValues = FieldData.unwrapSingleton(numericDocValues);
        }

        @Override
        public Block readValues(IntVector docs) throws IOException {
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
        public int docID() {
            return docID;
        }
    }

    private static class BytesValuesReader extends BlockDocValuesReader {
        private int docID = -1;
        private final SortedBinaryDocValues binaryDV;

        BytesValuesReader(SortedBinaryDocValues binaryDV) {
            this.binaryDV = binaryDV;
        }

        @Override
        public Block readValues(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            var blockBuilder = BytesRefBlock.newBytesRefBlockBuilder(positionCount);
            int lastDoc = -1;
            for (int i = 0; i < docs.getPositionCount(); i++) {
                int doc = docs.getInt(i);
                // docs within same block must be in order
                if (lastDoc >= doc) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                if (binaryDV.advanceExact(doc)) {
                    int dvCount = binaryDV.docValueCount();
                    if (dvCount != 1) {
                        throw new IllegalStateException(
                            "multi-values not supported for now, could not read doc [" + doc + "] with [" + dvCount + "] values"
                        );
                    }
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
        public int docID() {
            return docID;
        }
    }
}
