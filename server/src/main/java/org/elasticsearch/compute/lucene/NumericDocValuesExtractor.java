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
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Operator that extracts numeric doc values from Lucene
 * out of pages that have been produced by {@link LuceneCollector}
 * and outputs them to a new column.
 */
@Experimental
public class NumericDocValuesExtractor implements Operator {

    private final List<IndexReader> indexReaders;
    private final int docChannel;
    private final int leafOrdChannel;
    private final int shardChannel;
    private final String field;

    private LeafReaderContext lastLeafReaderContext;
    private NumericDocValues lastNumericDocValues;
    private Thread lastThread;
    private int lastShard = -1;

    private Page lastPage;

    boolean finished;

    public record NumericDocValuesExtractorFactory(
        List<IndexReader> indexReaders,
        int docChannel,
        int leafOrdChannel,
        int shardChannel,
        String field
    ) implements OperatorFactory {

        @Override
        public Operator get() {
            return new NumericDocValuesExtractor(indexReaders, docChannel, leafOrdChannel, shardChannel, field);
        }

        @Override
        public String describe() {
            return "NumericDocValuesExtractor(field = " + field + ")";
        }
    }

    /**
     * Creates a new extractor
     * @param indexReader the index reader to use for extraction
     * @param docChannel the channel that contains the doc ids
     * @param leafOrdChannel the channel that contains the segment ordinal
     * @param field the lucene field to use
     */
    public NumericDocValuesExtractor(IndexReader indexReader, int docChannel, int leafOrdChannel, int shardChannel, String field) {
        this(List.of(indexReader), docChannel, leafOrdChannel, shardChannel, field);
    }

    public NumericDocValuesExtractor(List<IndexReader> indexReaders, int docChannel, int leafOrdChannel, int shardChannel, String field) {
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
            if (firstDoc <= lastNumericDocValues.docID()) {
                reinitializeDocValues();
            }
            long[] values = new long[docs.getPositionCount()];
            try {
                int lastDoc = -1;
                for (int i = 0; i < docs.getPositionCount(); i++) {
                    int doc = docs.getInt(i);
                    // docs within same block must be in order
                    if (lastDoc >= doc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    // disallow sparse fields for now
                    if (lastNumericDocValues.advance(doc) != doc) {
                        throw new IllegalStateException(
                            "sparse fields not supported for now, asked for " + doc + " but got " + lastNumericDocValues.docID()
                        );
                    }
                    values[i] = lastNumericDocValues.longValue();
                    lastDoc = doc;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            lastPage = page.appendBlock(new LongArrayBlock(values, docs.getPositionCount()));
        }
    }

    private void initState(int ord, int shard) {
        boolean reinitializeDV = false;
        if (lastShard != shard) {
            lastLeafReaderContext = null;
        }
        lastShard = shard;
        if (lastLeafReaderContext != null && lastLeafReaderContext.ord != ord) {
            lastLeafReaderContext = null;
        }
        if (lastLeafReaderContext == null) {
            lastLeafReaderContext = indexReaders.get(shard).getContext().leaves().get(ord);
            reinitializeDV = true;
        }
        if (lastLeafReaderContext.ord != ord) {
            throw new IllegalStateException("wrong ord id");
        }
        if (Thread.currentThread() != lastThread) {
            // reset iterator when executing thread changes
            reinitializeDV = true;
        }
        if (reinitializeDV) {
            reinitializeDocValues();
        }
    }

    private void reinitializeDocValues() {
        try {
            SortedNumericDocValues sortedNumericDocValues = DocValues.getSortedNumeric(lastLeafReaderContext.reader(), field);
            lastNumericDocValues = DocValues.unwrapSingleton(sortedNumericDocValues);
            lastThread = Thread.currentThread();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        lastLeafReaderContext = null;
        lastNumericDocValues = null;
        lastThread = null;
    }
}
