/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
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

    private BlockDocValuesReader lastReader;
    private int lastShard = -1;
    private int lastSegment = -1;

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
            int segment = leafOrd.getInt(0);
            int shard = shardOrd.getInt(0);
            int firstDoc = docs.getInt(0);
            try {
                if (lastShard != shard || lastSegment != segment || BlockDocValuesReader.canReuse(lastReader, firstDoc) == false) {
                    ValuesSource vs = valuesSources.get(shard);
                    ValuesSourceType vt = valuesSourceTypes.get(shard);
                    LeafReaderContext leafReaderContext = indexReaders.get(shard).leaves().get(segment);
                    lastReader = BlockDocValuesReader.createBlockReader(vs, vt, leafReaderContext);
                    lastShard = shard;
                    lastSegment = segment;
                }
                Block block = lastReader.readValues(docs);
                lastPage = page.appendBlock(block);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() {

    }
}
