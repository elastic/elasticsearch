/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Shared Lucene slices between Lucene operators.
 */
public final class LuceneSliceQueue {
    private static final int MAX_DOCS_PER_SLICE = 250_000; // copied from IndexSearcher
    private static final int MAX_SEGMENTS_PER_SLICE = 5; // copied from IndexSearcher

    private final int totalSlices;
    private final Queue<LuceneSlice> slices;

    private LuceneSliceQueue(List<LuceneSlice> slices) {
        this.totalSlices = slices.size();
        this.slices = new ConcurrentLinkedQueue<>(slices);
    }

    @Nullable
    public LuceneSlice nextSlice() {
        return slices.poll();
    }

    public int totalSlices() {
        return totalSlices;
    }

    public static LuceneSliceQueue create(
        List<SearchContext> searchContexts,
        Function<SearchContext, Weight> weightFunction,
        DataPartitioning dataPartitioning,
        int taskConcurrency
    ) {
        final List<LuceneSlice> slices = new ArrayList<>();
        for (int shardIndex = 0; shardIndex < searchContexts.size(); shardIndex++) {
            final SearchContext searchContext = searchContexts.get(shardIndex);
            final List<LeafReaderContext> leafContexts = searchContext.searcher().getLeafContexts();
            List<List<PartialLeafReaderContext>> groups = switch (dataPartitioning) {
                case SHARD -> Collections.singletonList(leafContexts.stream().map(PartialLeafReaderContext::new).toList());
                case SEGMENT -> segmentSlices(leafContexts);
                case DOC -> docSlices(searchContext.searcher().getIndexReader(), taskConcurrency);
            };
            final Weight[] cachedWeight = new Weight[1];
            final Supplier<Weight> weight = () -> {
                if (cachedWeight[0] == null) {
                    cachedWeight[0] = weightFunction.apply(searchContext);
                }
                return cachedWeight[0];
            };
            if (groups.size() > 1) {
                weight.get(); // eagerly build Weight once
            }
            for (List<PartialLeafReaderContext> group : groups) {
                slices.add(new LuceneSlice(shardIndex, searchContext, group, weight));
            }
        }
        return new LuceneSliceQueue(slices);
    }

    static List<List<PartialLeafReaderContext>> docSlices(IndexReader indexReader, int numSlices) {
        final int totalDocCount = indexReader.maxDoc();
        final int normalMaxDocsPerSlice = totalDocCount / numSlices;
        final int extraDocsInFirstSlice = totalDocCount % numSlices;
        final List<List<PartialLeafReaderContext>> slices = new ArrayList<>();
        int docsAllocatedInCurrentSlice = 0;
        List<PartialLeafReaderContext> currentSlice = null;
        int maxDocsPerSlice = normalMaxDocsPerSlice + extraDocsInFirstSlice;
        for (LeafReaderContext ctx : indexReader.leaves()) {
            final int numDocsInLeaf = ctx.reader().maxDoc();
            int minDoc = 0;
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
                if (docsAllocatedInCurrentSlice == maxDocsPerSlice) {
                    slices.add(currentSlice);
                    maxDocsPerSlice = normalMaxDocsPerSlice; // once the first slice with the extra docs is added, no need for extra docs
                    currentSlice = null;
                    docsAllocatedInCurrentSlice = 0;
                }
            }
        }
        if (currentSlice != null) {
            slices.add(currentSlice);
        }
        if (numSlices < totalDocCount && slices.size() != numSlices) {
            throw new IllegalStateException("wrong number of slices, expected " + numSlices + " but got " + slices.size());
        }
        if (slices.stream()
            .flatMapToInt(
                l -> l.stream().mapToInt(partialLeafReaderContext -> partialLeafReaderContext.maxDoc() - partialLeafReaderContext.minDoc())
            )
            .sum() != totalDocCount) {
            throw new IllegalStateException("wrong doc count");
        }
        return slices;
    }

    static List<List<PartialLeafReaderContext>> segmentSlices(List<LeafReaderContext> leafContexts) {
        IndexSearcher.LeafSlice[] gs = IndexSearcher.slices(leafContexts, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
        return Arrays.stream(gs).map(g -> Arrays.stream(g.leaves).map(PartialLeafReaderContext::new).toList()).toList();
    }
}
