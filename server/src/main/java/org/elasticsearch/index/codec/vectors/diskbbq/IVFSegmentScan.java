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
 * Decides which posting lists one segment's IVF search visits: best centroid first, up to the visit budget, then on until
 * the collector is full and, with filters or deletes, until enough docs were scored. {@link #searchFrom} is that search, as
 * one thread runs it.
 * <p>
 * Keeping that decision here, apart from the visitors that score the posting lists, lets several threads search one segment
 * and still visit exactly what a single thread would. They share a scan. Each thread searching the segment is a "part": it
 * has its own visitor and collector, and the parts' collectors must be of the same kind and size.
 * <ul>
 *   <li>Within the budget, which posting lists to visit depends only on their sizes. They are listed once, and the parts
 *   take them one by one, without a lock.</li>
 *   <li>Past the budget, how far to go depends on what was found. So once every posting list within the budget was visited,
 *   a single part gathers the hits of all the parts into its collector and goes on with {@link #searchFrom}, from where the
 *   parts stopped.</li>
 * </ul>
 * Once the segment is planned and its budget is listed, no part waits for another.
 */
public final class IVFSegmentScan {

    /** What a search must know about a segment before it visits any posting list. The budget counts posting list entries. */
    record Plan(CentroidIterator postings, long budget, int numVectors, float percentFiltered, Bits acceptBits) {}

    /** The scan's posting lists within the budget, in visiting order, and how much of the budget they use. */
    private record Listed(PostingMetadata[] postings, long budgetUsed) {}

    private final ConcurrentMap<Integer, QuantizedQuery> quantizedQueries = new ConcurrentHashMap<>();
    private volatile boolean failed;
    private volatile boolean claimsHeld;

    // Built once, by the first search to arrive; guarded by this. One thread at a time advances its posting order: the part
    // that lists the budget, then the part that goes on past it, which starts from what was listed.
    private Plan plan;
    private boolean planned;

    // what the scan listed, and how many of those posting lists the parts have taken
    private volatile Listed listed;
    private final AtomicInteger taken = new AtomicInteger();
    // whether a part goes on past the budget, and the collector it gathered every part's hits into
    private final AtomicBoolean pastBudget = new AtomicBoolean();
    private KnnCollector finalCollector; // guarded by this
    private final List<KnnCollector> collectors = new ArrayList<>(); // of the parts that joined; guarded by this

    // what the parts did within the budget: posting lists visited to the end, and docs scored
    private final AtomicInteger visited = new AtomicInteger();
    private final AtomicLong scored = new AtomicLong();

    /** The query quantized per centroid by the parts' visitors, shared so that each centroid's is computed once. */
    public ConcurrentMap<Integer, QuantizedQuery> quantizedQueries() {
        return quantizedQueries;
    }

    /** Plans the search with {@code planner}, unless a part already did. Returns false if the segment has nothing to search. */
    synchronized boolean planOnce(IOSupplier<Plan> planner) throws IOException {
        if (planned == false) {
            planned = true;
            plan = planner.get();
        }
        return plan != null && failed == false;
    }

    /** The docs the search may score, shared read-only by every visitor of this segment. */
    synchronized Bits acceptBits() {
        return plan.acceptBits();
    }

    /** A part joins the scan with the collector it fills, and gets the plan. */
    private synchronized Plan join(KnnCollector collector) {
        collectors.add(collector);
        return plan;
    }

    /**
     * Once the search is over: the collector that holds the segment's hits, which is the one the hits of all the parts were
     * gathered into. Null if no part visited anything.
     */
    public synchronized KnnCollector finalCollector() {
        return finalCollector;
    }

    /** Once the search is over: how many vectors its parts visited, all together. */
    public synchronized long visitedCount() {
        long total = 0;
        for (KnnCollector collector : collectors) {
            total += collector.visitedCount();
        }
        return total;
    }

    /**
     * True when a part starting now would find nothing to visit: the search failed, a part already goes on past the budget,
     * or every posting list within the budget is taken and some are still being visited. Parts that start late check this
     * before they allocate anything.
     */
    public boolean nothingToClaim() {
        Listed listed = this.listed;
        boolean budgetTaken = listed != null && taken.get() >= listed.postings().length && visited.get() < listed.postings().length;
        return failed || pastBudget.get() || budgetTaken;
    }

    /**
     * While claims are held, a search of the segment plans it and lists its budget, but visits nothing. Preparing a segment
     * this way before its parts start means they find their work waiting, rather than wake up to wait for the plan. Must not
     * be called while parts are searching.
     */
    public void holdClaims() {
        claimsHeld = true;
    }

    public void releaseClaims() {
        claimsHeld = false;
    }

    /** Stops the other parts within the budget: one of them failed, so the query fails and the rest of their work would be wasted. */
    void fail() {
        failed = true;
    }

    /**
     * Searches the segment as one part of the scan, with {@code visitor}, into {@code collector}. The parts take the posting
     * lists within the budget one by one, and a single part goes on past them, as a search on its own does from the start.
     */
    void search(PostingVisitor visitor, KnnCollector collector) throws IOException {
        Plan plan = join(collector);
        Listed listed = this.listed != null ? this.listed : listBudget(visitor);
        if (listed == null || claimsHeld) {
            return;
        }
        PostingMetadata[] withinBudget = listed.postings();
        for (int next = taken.getAndIncrement(); next < withinBudget.length && failed == false; next = taken.getAndIncrement()) {
            visitor.resetPostingsScorer(withinBudget[next]);
            int scoredDocs = visit(visitor, collector);
            // Recorded last, when this thread is done with its collector: once every posting list within the budget is recorded,
            // another part may gather the collector's hits. The scored docs first: whoever then sees every posting list visited
            // also sees all their scored docs.
            scored.addAndGet(scoredDocs);
            visited.incrementAndGet();
        }
        // Past the budget only one part goes on: the first to get here once every posting list within it was visited.
        if (failed || visited.get() < withinBudget.length || pastBudget.compareAndSet(false, true) == false) {
            return;
        }
        gatherHitsInto(collector);
        searchFrom(plan, visitor, collector, listed.budgetUsed(), scored.get());
    }

    /**
     * Searches a segment as one thread does, from what was already done: {@code budgetUsed} posting list entries visited and
     * {@code scored} docs scored. A search on its own starts from zero. The part of a scan that goes on past the budget
     * starts from what the parts did within it.
     */
    static void searchFrom(Plan plan, PostingVisitor visitor, KnnCollector collector, long budgetUsed, long scored) throws IOException {
        CentroidIterator postings = plan.postings();
        // Within the budget, and on until the collector is full.
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        while (postings.hasNext() && (budgetUsed < plan.budget() || collector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY)) {
            budgetUsed += visitor.resetPostingsScorer(postings.nextPosting());
            scored += visit(visitor, collector);
        }
        if (plan.acceptBits() == null) {
            return;
        }
        // Filters and deletes skip docs, so keep going until enough docs were scored: twice the filter's share of the posting
        // list entries visited so far, and at most half of them.
        // TODO Adjust the value here when using centroid filtering
        float unfilteredRatioVisited = (float) budgetUsed / plan.numVectors();
        int filteredVectors = (int) Math.ceil(plan.numVectors() * plan.percentFiltered());
        float expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, budgetUsed / 2f);
        while (postings.hasNext() && (scored < expectedScored || scored < collector.k())) {
            visitor.resetPostingsScorer(postings.nextPosting());
            scored += visit(visitor, collector);
        }
    }

    /** Visits the posting list {@code visitor} was moved onto. Returns how many docs it scored. */
    private static int visit(PostingVisitor visitor, KnnCollector collector) throws IOException {
        int scoredDocs = visitor.visit(collector);
        if (collector.getSearchStrategy() != null) {
            collector.getSearchStrategy().nextVectorsBlock();
        }
        return scoredDocs;
    }

    /** Lists the posting lists within the budget, in visiting order, so that parts can take them by index. */
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

    /**
     * Moves the hits of every other part into {@code collector}, which then holds what the collector of a search on its own
     * would hold. So the part that goes on can ask its collector whether it is full, whatever the collector counts: docs, or
     * parent docs for nested fields. The other parts are done with their collectors by now: each recorded its last posting
     * list after it last used its collector.
     */
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
