/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSink;

public class LuceneCollector extends SimpleCollector {
    private static final int PAGE_SIZE = 4096;

    private final int pageSize;
    private int[] currentPage;
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
        if (currentPage == null) {
            currentPage = new int[pageSize];
            currentPos = 0;
        }
        currentPage[currentPos] = doc;
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
            Page page = new Page(currentPos, new IntBlock(currentPage, currentPos), new ConstantIntBlock(currentPos, lastContext.ord));
            exchangeSink.waitForWriting().actionGet();
            exchangeSink.addPage(page);
        }
        currentPage = null;
        currentPos = 0;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public void finish() {
        createPage();
        exchangeSink.finish();
    }
}
