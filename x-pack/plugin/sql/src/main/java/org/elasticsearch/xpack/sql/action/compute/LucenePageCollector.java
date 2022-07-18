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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LucenePageCollector extends SimpleCollector implements Operator {

    private static final int PAGE_SIZE = 4096;

    private final int pageSize;
    private int[] currentPage;
    private int currentPos;
    private LeafReaderContext lastContext;
    private volatile boolean finished;

    public final BlockingQueue<Page> pages = new LinkedBlockingQueue<>(2);

    public LucenePageCollector() {
        this(PAGE_SIZE);
    }

    public LucenePageCollector(int pageSize) {
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
            try {
                pages.put(page);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        currentPage = null;
        currentPos = 0;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void finish() {
        assert finished == false;
        createPage();
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }

    @Override
    public Page getOutput() {
        return pages.poll();
    }

    @Override
    public boolean isFinished() {
        return finished && pages.isEmpty();
    }
}
