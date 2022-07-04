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

import java.util.ArrayList;
import java.util.List;

public class PageCollector extends SimpleCollector implements Operator {

    public static final int PAGE_SIZE = 4096;

    private int[] currentPage;
    private int currentPos;
    private LeafReaderContext lastContext;
    private boolean finished;

    public final List<Page> pages = new ArrayList<>(); // TODO: use queue

    PageCollector() {}

    @Override
    public void collect(int doc) {
        if (currentPage == null) {
            currentPage = new int[PAGE_SIZE];
            currentPos = 0;
        }
        currentPage[currentPos] = doc;
        currentPos++;
        if (currentPos == PAGE_SIZE) {
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

    private synchronized void createPage() {
        if (currentPos > 0) {
            Page page = new Page(currentPos, new IntBlock(currentPage, currentPos));
            pages.add(page);
        }
        currentPage = null;
        currentPos = 0;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public synchronized void finish() {
        createPage();
        finished = true;
    }

    @Override
    public synchronized Page getOutput() {
        if (pages.isEmpty()) {
            return null;
        }
        return pages.remove(0);
    }

    @Override
    public synchronized boolean isFinished() {
        return finished;
    }
}
