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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

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

    public Iterable<LuceneSlice> getSlices() {
        return slices;
    }

    public static LuceneSliceQueue create(
        List<? extends ShardContext> contexts,
        Function<ShardContext, Weight> weightFunction,
        DataPartitioning dataPartitioning,
        int taskConcurrency
    ) {
        final List<LuceneSlice> slices = new ArrayList<>();
        for (ShardContext ctx : contexts) {
            final List<LeafReaderContext> leafContexts = ctx.searcher().getLeafContexts();
            List<List<PartialLeafReaderContext>> groups = switch (dataPartitioning) {
                case SHARD -> Collections.singletonList(leafContexts.stream().map(PartialLeafReaderContext::new).toList());
                case SEGMENT -> segmentSlices(leafContexts);
                case DOC -> docSlices(ctx.searcher().getIndexReader(), taskConcurrency);
            };
            final Weight weight = weightFunction.apply(ctx);
            for (List<PartialLeafReaderContext> group : groups) {
                if (group.isEmpty() == false) {
                    slices.add(new LuceneSlice(ctx, group, weight));
                }
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
        IndexSearcher.LeafSlice[] gs = IndexSearcher.slices(leafContexts, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE, false);
        return Arrays.stream(gs).map(g -> Arrays.stream(g.partitions).map(PartialLeafReaderContext::new).toList()).toList();
    }
}
