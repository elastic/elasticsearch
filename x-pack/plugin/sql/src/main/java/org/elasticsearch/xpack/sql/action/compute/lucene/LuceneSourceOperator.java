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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.sql.action.compute.data.ConstantIntBlock;
import org.elasticsearch.xpack.sql.action.compute.data.IntBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Source operator that incrementally runs Lucene searches
 */
public class LuceneSourceOperator implements Operator {

    private static final int PAGE_SIZE = ByteSizeValue.ofKb(16).bytesAsInt();

    @Nullable
    private final IndexReader indexReader;
    private final int shardId;
    @Nullable
    private final Query query;
    private final List<PartialLeafReaderContext> leaves;
    private final int maxPageSize;
    private final int minPageSize;

    private Weight weight;

    private int currentLeaf = 0;
    private PartialLeafReaderContext currentLeafReaderContext = null;
    private BulkScorer currentScorer = null;

    private int currentPagePos;
    private final int[] currentPage;

    private int currentScorerPos;

    public LuceneSourceOperator(IndexReader reader, int shardId, Query query) {
        this(reader, shardId, query, PAGE_SIZE);
    }

    public LuceneSourceOperator(IndexReader reader, int shardId, Query query, int maxPageSize) {
        this.indexReader = reader;
        this.shardId = shardId;
        this.leaves = reader.leaves().stream().map(PartialLeafReaderContext::new).collect(Collectors.toList());
        this.query = query;
        this.maxPageSize = maxPageSize;
        this.minPageSize = maxPageSize / 2;
        currentPage = new int[maxPageSize];
    }

    private LuceneSourceOperator(Weight weight, int shardId, List<PartialLeafReaderContext> leaves, int maxPageSize) {
        this.indexReader = null;
        this.shardId = shardId;
        this.leaves = leaves;
        this.query = null;
        this.weight = weight;
        this.maxPageSize = maxPageSize;
        this.minPageSize = maxPageSize / 2;
        currentPage = new int[maxPageSize];
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
        return currentLeaf >= leaves.size();
    }

    /**
     * Split this source operator into a given number of slices
     */
    public List<LuceneSourceOperator> docSlice(int numSlices) {
        if (weight != null) {
            throw new IllegalStateException("can only call slice method once");
        }
        initializeWeightIfNecessary();

        List<LuceneSourceOperator> operators = new ArrayList<>();
        for (List<PartialLeafReaderContext> slice : docSlices(indexReader, numSlices)) {
            operators.add(new LuceneSourceOperator(weight, shardId, slice, maxPageSize));
        }
        return operators;
    }

    public static int numDocSlices(IndexReader indexReader, int numSlices) {
        return docSlices(indexReader, numSlices).size();
    }

    private static List<List<PartialLeafReaderContext>> docSlices(IndexReader indexReader, int numSlices) {
        final int totalDocCount = indexReader.maxDoc();
        final int maxDocsPerSlice = totalDocCount % numSlices == 0 ? totalDocCount / numSlices : (totalDocCount / numSlices) + 1;

        final List<List<PartialLeafReaderContext>> slices = new ArrayList<>();
        int docsAllocatedInCurrentSlice = 0;
        List<PartialLeafReaderContext> currentSlice = null;
        for (LeafReaderContext ctx : indexReader.leaves()) {
            int minDoc = 0;
            int numDocsInLeaf = ctx.reader().maxDoc();
            while (minDoc < numDocsInLeaf) {
                int numDocsToUse = Math.min(maxDocsPerSlice - docsAllocatedInCurrentSlice, numDocsInLeaf - minDoc);
                if (numDocsToUse <= 0) {
                    break;
                }
                if (currentSlice == null) {
                    currentSlice = new ArrayList<>();
                }
                currentSlice.add(new PartialLeafReaderContext(ctx, minDoc, minDoc + numDocsToUse));
                minDoc += numDocsToUse;
                docsAllocatedInCurrentSlice += numDocsToUse;
                if (docsAllocatedInCurrentSlice >= maxDocsPerSlice) {
                    slices.add(currentSlice);
                    currentSlice = null;
                    docsAllocatedInCurrentSlice = 0;
                }
            }
        }
        if (currentSlice != null) {
            slices.add(currentSlice);
        }
        if (slices.size() != numSlices) {
            throw new IllegalStateException("wrong number of slices, expected " + numSlices + " but got " + slices.size());
        }
        return slices;
    }

    /**
     * Uses Lucene's own slicing method, which creates per-segment level slices
     */
    public List<LuceneSourceOperator> segmentSlice() {
        if (weight != null) {
            throw new IllegalStateException("can only call slice method once");
        }
        initializeWeightIfNecessary();
        List<LuceneSourceOperator> operators = new ArrayList<>();
        for (IndexSearcher.LeafSlice leafSlice : segmentSlices(indexReader)) {
            operators.add(
                new LuceneSourceOperator(
                    weight,
                    shardId,
                    Arrays.asList(leafSlice.leaves).stream().map(PartialLeafReaderContext::new).collect(Collectors.toList()),
                    maxPageSize
                )
            );
        }
        return operators;
    }

    private static IndexSearcher.LeafSlice[] segmentSlices(IndexReader indexReader) {
        return IndexSearcher.slices(indexReader.leaves(), MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
    }

    public static int numSegmentSlices(IndexReader indexReader) {
        return segmentSlices(indexReader).length;
    }

    private static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    private static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }

        // initialize weight if not done yet
        initializeWeightIfNecessary();

        Page page = null;

        // initializes currentLeafReaderContext, currentScorer, and currentScorerPos when we switch to a new leaf reader
        if (currentLeafReaderContext == null) {
            currentLeafReaderContext = leaves.get(currentLeaf);
            try {
                currentScorer = weight.bulkScorer(currentLeafReaderContext.leafReaderContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            currentScorerPos = currentLeafReaderContext.minDoc;
        }

        try {
            currentScorerPos = currentScorer.score(new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) {
                    currentPage[currentPagePos] = doc;
                    currentPagePos++;
                }
            },
                currentLeafReaderContext.leafReaderContext.reader().getLiveDocs(),
                currentScorerPos,
                Math.min(currentLeafReaderContext.maxDoc, currentScorerPos + maxPageSize - currentPagePos)
            );

            if (currentPagePos >= minPageSize || currentScorerPos >= currentLeafReaderContext.maxDoc) {
                page = new Page(
                    currentPagePos,
                    new IntBlock(Arrays.copyOf(currentPage, currentPagePos), currentPagePos),
                    new ConstantIntBlock(currentPagePos, currentLeafReaderContext.leafReaderContext.ord),
                    new ConstantIntBlock(currentPagePos, shardId)
                );
                currentPagePos = 0;
            }

            if (currentScorerPos >= currentLeafReaderContext.maxDoc) {
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

    private void initializeWeightIfNecessary() {
        if (weight == null) {
            try {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                weight = indexSearcher.createWeight(indexSearcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.COMPLETE_NO_SCORES, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static class PartialLeafReaderContext {

        final LeafReaderContext leafReaderContext;
        final int minDoc; // incl
        final int maxDoc; // excl

        PartialLeafReaderContext(LeafReaderContext leafReaderContext, int minDoc, int maxDoc) {
            this.leafReaderContext = leafReaderContext;
            this.minDoc = minDoc;
            this.maxDoc = maxDoc;
        }

        PartialLeafReaderContext(LeafReaderContext leafReaderContext) {
            this(leafReaderContext, 0, leafReaderContext.reader().maxDoc());
        }

    }

    @Override
    public void close() {

    }
}
