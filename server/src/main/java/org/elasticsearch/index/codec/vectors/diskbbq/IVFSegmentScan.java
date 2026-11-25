/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.PostingVisitor;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.QuantizedQuery;
import org.elasticsearch.search.vectors.BulkKnnCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The posting lists one segment's IVF search visits: best centroid first up to the visit budget, then on until enough was
 * found ({@link #searchFrom}). Several threads ("parts"), each with its own visitor and collector, can share a scan and visit
 * what one thread would: within the budget they take posting lists from one list, without a lock, and past it one part
 * gathers every part's hits and goes on alone.
 */
public final class IVFSegmentScan {

    /** What a search needs before it visits a posting list. The budget is in posting list entries. */
    record Plan(CentroidIterator postings, long budget, int numVectors, float percentFiltered, Bits acceptBits) {}

    /** The posting lists within the budget, in visiting order, and the budget they use. */
    private record Listed(PostingMetadata[] postings, long budgetUsed) {}

    private final ConcurrentMap<Integer, QuantizedQuery> quantizedQueries = new ConcurrentHashMap<>();
    private volatile boolean failed;
    private volatile boolean preparing;

    // built once, guarded by this; one thread at a time advances its posting order
    private Plan plan;
    private boolean planned;

    private volatile Listed listed;
    private final AtomicInteger taken = new AtomicInteger(); // listed posting lists the parts took
    private final AtomicInteger visited = new AtomicInteger(); // ... and visited to the end
    private final AtomicLong scored = new AtomicLong(); // docs scored within the budget
    private final AtomicBoolean pastBudget = new AtomicBoolean(); // a part goes on past the budget
    private final List<KnnCollector> collectors = new ArrayList<>(); // of the parts; guarded by this
    private KnnCollector finalCollector; // holds every part's hits; guarded by this

    /** The query quantized per centroid, shared so that each centroid's is computed once. */
    public ConcurrentMap<Integer, QuantizedQuery> quantizedQueries() {
        return quantizedQueries;
    }

    /** Plans the search unless a part already did. False if the segment has nothing to search. */
    synchronized boolean planOnce(IOSupplier<Plan> planner) throws IOException {
        if (planned == false) {
            planned = true;
            plan = planner.get();
        }
        return plan != null && failed == false;
    }

    /** The docs the search may score, shared read-only by the parts. */
    synchronized Bits acceptBits() {
        return plan.acceptBits();
    }

    private synchronized Plan join(KnnCollector collector) {
        collectors.add(collector);
        return plan;
    }

    /** After the search: the collector holding the segment's hits, or null if no part visited anything. */
    public synchronized KnnCollector finalCollector() {
        return finalCollector;
    }

    /** After the search: how many vectors the parts visited in total. */
    public synchronized long visitedCount() {
        long total = 0;
        for (KnnCollector collector : collectors) {
            total += collector.visitedCount();
        }
        return total;
    }

    /** Runs {@code search} so that it plans the segment and lists its budget but visits nothing. Call before the parts start. */
    public <E extends Exception> void prepare(CheckedRunnable<E> search) throws E {
        preparing = true;
        try {
            search.run();
        } finally {
            preparing = false;
        }
    }

    /** A part failed, so the query fails: the other parts stop taking posting lists. */
    void fail() {
        failed = true;
    }

    /** Searches the segment as one part: takes posting lists within the budget, and may be the part that goes on past it. */
    void search(PostingVisitor visitor, KnnCollector collector) throws IOException {
        Plan plan = join(collector);
        Listed listed = this.listed != null ? this.listed : listBudget(visitor);
        if (listed == null || preparing) {
            return;
        }
        PostingMetadata[] withinBudget = listed.postings();
        for (int next = taken.getAndIncrement(); next < withinBudget.length && failed == false; next = taken.getAndIncrement()) {
            visitor.resetPostingsScorer(withinBudget[next]);
            int scoredDocs = visit(visitor, collector);
            // Recorded last, the scored docs first: once every list is recorded, another part may take this collector's hits.
            scored.addAndGet(scoredDocs);
            visited.incrementAndGet();
        }
        // Only one part goes on past the budget: the first here once every listed posting list was visited.
        if (failed || visited.get() < withinBudget.length || pastBudget.compareAndSet(false, true) == false) {
            return;
        }
        gatherHitsInto(collector);
        searchFrom(plan, visitor, collector, listed.budgetUsed(), scored.get());
    }

    /** Searches a segment as one thread does, from {@code budgetUsed} entries visited and {@code scored} docs scored. */
    static void searchFrom(Plan plan, PostingVisitor visitor, KnnCollector collector, long budgetUsed, long scored) throws IOException {
        CentroidIterator postings = plan.postings();
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        while (postings.hasNext() && (budgetUsed < plan.budget() || collector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY)) {
            budgetUsed += visitor.resetPostingsScorer(postings.nextPosting());
            scored += visit(visitor, collector);
        }
        if (plan.acceptBits() == null) {
            return;
        }
        // Filters and deletes skip docs: go on until twice the filter's share of the visited entries is scored, at most half.
        // TODO Adjust the value here when using centroid filtering
        float unfilteredRatioVisited = (float) budgetUsed / plan.numVectors();
        int filteredVectors = (int) Math.ceil(plan.numVectors() * plan.percentFiltered());
        float expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, budgetUsed / 2f);
        while (postings.hasNext() && (scored < expectedScored || scored < collector.k())) {
            visitor.resetPostingsScorer(postings.nextPosting());
            scored += visit(visitor, collector);
        }
    }

    /** Visits the posting list the visitor is on. Returns how many docs it scored. */
    private static int visit(PostingVisitor visitor, KnnCollector collector) throws IOException {
        int scoredDocs = visitor.visit(collector);
        if (collector.getSearchStrategy() != null) {
            collector.getSearchStrategy().nextVectorsBlock();
        }
        return scoredDocs;
    }

    private synchronized Listed listBudget(PostingVisitor visitor) throws IOException {
        if (listed == null && failed == false) {
            List<PostingMetadata> postings = new ArrayList<>();
            long budgetUsed = 0;
            while (plan.postings().hasNext() && budgetUsed < plan.budget()) {
                PostingMetadata posting = plan.postings().nextPosting();
                postings.add(posting);
                // a posting list's size is only known once a visitor is moved onto it
                budgetUsed += visitor.resetPostingsScorer(posting);
            }
            listed = new Listed(postings.toArray(PostingMetadata[]::new), budgetUsed);
        }
        return listed;
    }

    /** Moves every other part's hits into {@code collector}, which can then tell whether the search found enough. */
    private synchronized void gatherHitsInto(KnnCollector collector) {
        for (KnnCollector other : collectors) {
            if (other != collector) {
                TopDocs hits = other instanceof BulkKnnCollector bulk ? bulk.unsortedTopK() : other.topDocs();
                if (hits != null) {
                    collect(collector, hits.scoreDocs);
                }
            }
        }
        finalCollector = collector;
    }

    private static void collect(KnnCollector collector, ScoreDoc[] hits) {
        if (hits.length > 0 && collector instanceof BulkKnnCollector bulk) {
            int[] docs = new int[hits.length];
            float[] scores = new float[hits.length];
            float best = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < hits.length; i++) {
                docs[i] = hits[i].doc;
                scores[i] = hits[i].score;
                best = Math.max(best, scores[i]);
            }
            bulk.bulkCollect(docs, scores, hits.length, best);
        } else {
            for (ScoreDoc hit : hits) {
                collector.collect(hit.doc, hit.score);
            }
        }
    }
}
