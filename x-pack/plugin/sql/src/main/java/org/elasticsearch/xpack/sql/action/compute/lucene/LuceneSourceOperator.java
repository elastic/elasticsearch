/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.xpack.sql.action.compute.data.ConstantIntBlock;
import org.elasticsearch.xpack.sql.action.compute.data.IntBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Source operator that incrementally runs Lucene searches
 */
public class LuceneSourceOperator implements Operator {

    private static final int PAGE_SIZE = 4096;

    private final IndexReader reader;
    private final Query query;
    private final int maxPageSize;
    private final int minPageSize;

    private Weight weight;

    private int currentLeaf = 0;
    private LeafReaderContext currentLeafReaderContext = null;
    private BulkScorer currentScorer = null;

    private int currentPagePos;
    private int[] currentPage;

    private int currentScorerPos;

    public LuceneSourceOperator(IndexReader reader, Query query) {
        this(reader, query, PAGE_SIZE);
    }

    public LuceneSourceOperator(IndexReader reader, Query query, int maxPageSize) {
        this.reader = reader;
        this.query = query;
        this.maxPageSize = maxPageSize;
        this.minPageSize = maxPageSize / 2;
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
    public void finish() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinished() {
        return currentLeaf >= reader.leaves().size();
    }

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }

        // initialize weight if not done yet
        if (weight == null) {
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            try {
                weight = indexSearcher.createWeight(indexSearcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.COMPLETE_NO_SCORES, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        Page page = null;

        // initializes currentLeafReaderContext, currentScorer, and currentScorerPos when we switch to a new leaf reader
        if (currentLeafReaderContext == null) {
            currentLeafReaderContext = reader.leaves().get(currentLeaf);
            try {
                currentScorer = weight.bulkScorer(currentLeafReaderContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            currentScorerPos = 0;
        }

        try {
            currentScorerPos = currentScorer.score(new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) {
                    if (currentPage == null) {
                        currentPage = new int[maxPageSize];
                        currentPagePos = 0;
                    }
                    currentPage[currentPagePos] = doc;
                    currentPagePos++;
                }
            }, currentLeafReaderContext.reader().getLiveDocs(), currentScorerPos, currentScorerPos + maxPageSize - currentPagePos);

            if (currentPagePos >= minPageSize || currentScorerPos == DocIdSetIterator.NO_MORE_DOCS) {
                page = new Page(
                    currentPagePos,
                    new IntBlock(currentPage, currentPagePos),
                    new ConstantIntBlock(currentPagePos, currentLeafReaderContext.ord)
                );
                currentPage = null;
                currentPagePos = 0;
            }

            if (currentScorerPos == DocIdSetIterator.NO_MORE_DOCS) {
                currentLeaf++;
                currentLeafReaderContext = null;
                currentScorer = null;
                currentScorerPos = 0;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return page;
    }

    @Override
    public void close() {

    }
}
