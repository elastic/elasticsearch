/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntBlock;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorFactory;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Operator that extracts doc_values from a Lucene index out of pages that have been produced by {@link LuceneSourceOperator}
 * and outputs them to a new column. The operator leverages the {@link ValuesSource} infrastructure for extracting
 * field values. This allows for a more uniform way of extracting data compared to deciding the correct doc_values
 * loader for different field types.
 */
@Experimental
public class ValuesSourceReaderOperator implements Operator {

    private final List<ValuesSourceType> valuesSourceTypes;
    private final List<ValuesSource> valuesSources;
    private final List<IndexReader> indexReaders;
    private final int docChannel;
    private final int leafOrdChannel;
    private final int shardChannel;
    private final String field;

    private LeafReaderContext lastLeafReaderContext;
    private DocValuesCollector docValuesCollector;
    private ValuesSource lastValuesSource;
    private ValuesSourceType lastValuesSourceType;
    private Thread lastThread;
    private int lastShard = -1;

    private Page lastPage;

    boolean finished;

    /**
     * Creates a new extractor that uses ValuesSources load data
     * @param indexReaders the index readers to use for extraction
     * @param docChannel the channel that contains the doc ids
     * @param leafOrdChannel the channel that contains the segment ordinal
     * @param field the lucene field to use
     */
    public record ValuesSourceReaderOperatorFactory(
        List<ValuesSourceType> valuesSourceTypes,
        List<ValuesSource> valuesSources,
        List<IndexReader> indexReaders,
        int docChannel,
        int leafOrdChannel,
        int shardChannel,
        String field
    ) implements OperatorFactory {

        @Override
        public Operator get() {
            return new ValuesSourceReaderOperator(
                valuesSourceTypes,
                valuesSources,
                indexReaders,
                docChannel,
                leafOrdChannel,
                shardChannel,
                field
            );
        }

        @Override
        public String describe() {
            return "ValuesSourceReaderOperator(field = " + field + ")";
        }
    }

    /**
     * Creates a new extractor
     * @param valuesSources the {@link ValuesSource} instances to use for extraction
     * @param indexReaders the index readers to use for extraction
     * @param docChannel the channel that contains the doc ids
     * @param leafOrdChannel the channel that contains the segment ordinal
     * @param field the lucene field to use
     */
    public ValuesSourceReaderOperator(
        List<ValuesSourceType> valuesSourceTypes,
        List<ValuesSource> valuesSources,
        List<IndexReader> indexReaders,
        int docChannel,
        int leafOrdChannel,
        int shardChannel,
        String field
    ) {
        this.valuesSourceTypes = valuesSourceTypes;
        this.valuesSources = valuesSources;
        this.indexReaders = indexReaders;
        this.docChannel = docChannel;
        this.leafOrdChannel = leafOrdChannel;
        this.shardChannel = shardChannel;
        this.field = field;
    }

    @Override
    public Page getOutput() {
        Page l = lastPage;
        lastPage = null;
        return l;
    }

    @Override
    public boolean isFinished() {
        return finished && lastPage == null;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return lastPage == null;
    }

    @Override
    public void addInput(Page page) {
        Block docs = page.getBlock(docChannel);
        ConstantIntBlock leafOrd = (ConstantIntBlock) page.getBlock(leafOrdChannel);
        ConstantIntBlock shardOrd = (ConstantIntBlock) page.getBlock(shardChannel);

        if (docs.getPositionCount() > 0) {
            int ord = leafOrd.getInt(0);
            int shard = shardOrd.getInt(0);
            initState(ord, shard);
            int firstDoc = docs.getInt(0);
            // reset iterator when blocks arrive out-of-order
            if (firstDoc <= docValuesCollector.docID()) {
                resetDocValues();
            }

            try {
                docValuesCollector.initBlock(docs.getPositionCount());
                int lastDoc = -1;
                for (int i = 0; i < docs.getPositionCount(); i++) {
                    int doc = docs.getInt(i);
                    // docs within same block must be in order
                    if (lastDoc >= doc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    docValuesCollector.collect(doc);
                    lastDoc = doc;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            lastPage = page.appendBlock(docValuesCollector.createBlock());
        }
    }

    private void initState(int ord, int shard) {
        boolean resetDV = false;
        if (lastShard != shard) {
            lastLeafReaderContext = null;
            lastShard = shard;
        }
        if (lastLeafReaderContext != null && lastLeafReaderContext.ord != ord) {
            lastLeafReaderContext = null;
            lastValuesSource = null;
            lastValuesSourceType = null;
        }
        if (lastLeafReaderContext == null || lastValuesSource == null) {
            lastLeafReaderContext = indexReaders.get(shard).getContext().leaves().get(ord);
            lastValuesSource = valuesSources.get(shard);
            lastValuesSourceType = valuesSourceTypes.get(shard);
            resetDV = true;
        }
        if (lastLeafReaderContext.ord != ord) {
            throw new IllegalStateException("wrong ord id");
        }
        if (Thread.currentThread() != lastThread) {
            // reset iterator when executing thread changes
            resetDV = true;
        }
        if (resetDV) {
            resetDocValues();
        }
    }

    private void resetDocValues() {
        try {
            if (CoreValuesSourceType.NUMERIC.equals(lastValuesSourceType) || CoreValuesSourceType.DATE.equals(lastValuesSourceType)) {
                ValuesSource.Numeric numericVS = (ValuesSource.Numeric) lastValuesSource;
                if (numericVS.isFloatingPoint()) {
                    // Extract double values
                    SortedNumericDoubleValues sortedNumericDocValues = numericVS.doubleValues(lastLeafReaderContext);
                    final NumericDoubleValues numericDocValues = FieldData.unwrapSingleton(sortedNumericDocValues);
                    this.docValuesCollector = new DocValuesCollector() {
                        private double[] values;
                        private int positionCount;
                        private int i;

                        /**
                         * Store docID internally because class {@link NumericDoubleValues} does not support
                         * a docID() method.
                         */
                        private int docID = -1;

                        @Override
                        public void initBlock(int positionCount) {
                            this.i = 0;
                            this.positionCount = positionCount;
                            this.values = new double[positionCount];
                        }

                        @Override
                        public int docID() {
                            return docID;
                        }

                        @Override
                        public void collect(int doc) throws IOException {
                            if (numericDocValues.advanceExact(doc) == false) {
                                throw new IllegalStateException("sparse fields not supported for now, could not read doc [" + doc + "]");
                            }
                            values[i++] = numericDocValues.doubleValue();
                            docID = doc;
                        }

                        @Override
                        public Block createBlock() {
                            Block block = new DoubleArrayBlock(values, positionCount);
                            // Set values[] to null to protect from overwriting this memory by subsequent calls to collect()
                            // without calling initBlock() first
                            values = null;
                            return block;
                        }
                    };
                } else {
                    // Extract long values
                    SortedNumericDocValues sortedNumericDocValues = numericVS.longValues(lastLeafReaderContext);
                    final NumericDocValues numericDocValues = DocValues.unwrapSingleton(sortedNumericDocValues);
                    this.docValuesCollector = new DocValuesCollector() {
                        private long[] values;
                        private int positionCount;
                        private int i;

                        @Override
                        public void initBlock(int positionCount) {
                            this.values = new long[positionCount];
                            this.positionCount = positionCount;
                            this.i = 0;
                        }

                        @Override
                        public int docID() {
                            return numericDocValues.docID();
                        }

                        @Override
                        public void collect(int doc) throws IOException {
                            if (numericDocValues.advanceExact(doc) == false) {
                                throw new IllegalStateException("sparse fields not supported for now, could not read doc [" + doc + "]");
                            }
                            values[i++] = numericDocValues.longValue();
                        }

                        @Override
                        public Block createBlock() {
                            Block block = new LongArrayBlock(values, positionCount);
                            // Set values[] to null to protect from overwriting this memory by subsequent calls to collect()
                            // without calling initBlock() first
                            values = null;
                            return block;
                        }
                    };
                }
            } else {
                throw new IllegalArgumentException("Field type [" + lastValuesSourceType.typeName() + "] is not supported");
            }
            lastThread = Thread.currentThread();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        lastLeafReaderContext = null;
        lastValuesSource = null;
        docValuesCollector = null;
        lastThread = null;
    }

    /**
     * Interface that collects documents, extracts its doc_value data and creates a
     * {@link Block} with all extracted values.
     */
    interface DocValuesCollector {

        /**
         * Initialize {@link Block} memory for storing values. It must always be called
         * before collecting documents for a new block.
         * @param positionCount the position count for the block
         */
        void initBlock(int positionCount);

        /**
         * Collect the given {@code doc}
         */
        void collect(int doc) throws IOException;

        /**
         * Returns the following:
         * -1 if nextDoc() or advance(int) were not called yet.
         * NO_MORE_DOCS if the iterator has exhausted.
         * Otherwise, it should return the doc ID it is currently on.
         */
        int docID();

        /**
         * Create a block containing all extracted values for the collected documents
         * @return a {@link Block} with all values
         */
        Block createBlock();
    }
}
