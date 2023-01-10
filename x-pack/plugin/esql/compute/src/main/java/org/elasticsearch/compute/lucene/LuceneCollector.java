/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;

import static org.elasticsearch.compute.data.BlockBuilder.newConstantIntBlockWith;

/**
 * Lucene {@link org.apache.lucene.search.Collector} that turns collected docs
 * into {@link Page}s and sends them to an {@link ExchangeSink}. The pages
 * contain a block with the doc ids as well as block with the corresponding
 * segment ordinal where the doc was collected from.
 */
@Experimental
public class LuceneCollector extends SimpleCollector {
    private static final int PAGE_SIZE = 4096;

    private final int pageSize;
    private BlockBuilder currentBlockBuilder;
    private int currentPos;
    private LeafReaderContext lastContext;
    private final ExchangeSink exchangeSink;

    public LuceneCollector(ExchangeSink exchangeSink) {
        this(exchangeSink, PAGE_SIZE);
    }

    public LuceneCollector(ExchangeSink exchangeSink, int pageSize) {
        this.exchangeSink = exchangeSink;
        this.pageSize = pageSize;
    }

    @Override
    public void collect(int doc) {
        if (currentBlockBuilder == null) {
            currentBlockBuilder = BlockBuilder.newIntBlockBuilder(pageSize);
            currentPos = 0;
        }
        currentBlockBuilder.appendInt(doc);
        currentPos++;
        if (currentPos == pageSize) {
            createPage();
        }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) {
        if (context != lastContext) {
            createPage();
        }
        lastContext = context;
    }

    private void createPage() {
        if (currentPos > 0) {
            Page page = new Page(currentPos, currentBlockBuilder.build(), newConstantIntBlockWith(lastContext.ord, currentPos));
            exchangeSink.waitForWriting().actionGet();
            exchangeSink.addPage(page);
        }
        currentBlockBuilder = null;
        currentPos = 0;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * should be called once collection has completed
     */
    public void finish() {
        createPage();
        exchangeSink.finish();
    }
}
