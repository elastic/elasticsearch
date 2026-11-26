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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.PostingVisitor;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.QuantizedQuery;
import org.elasticsearch.search.vectors.BulkKnnCollector;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The posting lists one segment's IVF search visits: best centroid first up to the visit budget, then on until enough was
 * found ({@link #searchFrom}). Several threads ("parts"), each with its own visitor and collector, can share a scan and visit
 * what one thread would: within the budget they take posting lists in order, and past it one part gathers every part's hits
 * and goes on alone. No part waits for another: the order of the lists they take is there before they start.
 */
public final class IVFSegmentScan {

    /**
     * What a search needs before it visits a posting list. The budget is in posting list entries.
     * @param order the posting lists a scan's parts take, best first: every list the budget can reach. None for a search on
     *              its own.
     * @param rest  the posting lists after those, best first
     */
    record Plan(PostingMetadata[] order, CentroidIterator rest, long budget, int numVectors, float percentFiltered, Bits acceptBits) {}

    /** How far the parts are: the posting lists they took, in order, and the entries those hold. */
    private record Taken(int lists, long entries) {}

    private static final Plan NOTHING_TO_SEARCH = new Plan(new PostingMetadata[0], null, 0, 0, 0f, null);

    private final int parts;
    private final ConcurrentMap<Integer, QuantizedQuery> quantizedQueries = new ConcurrentHashMap<>();
    private volatile boolean preparing;
    private final AtomicReference<Plan> plan = new AtomicReference<>();

    private final AtomicReference<Taken> taken = new AtomicReference<>(new Taken(0, 0));
    private final AtomicInteger visited = new AtomicInteger(); // posting lists taken and visited to the end
    private final AtomicLong scored = new AtomicLong(); // docs scored within the budget
    private final AtomicBoolean pastBudget = new AtomicBoolean(); // a part goes on past the budget
    private final Queue<KnnCollector> collectors = new ConcurrentLinkedQueue<>(); // of the parts
    private volatile KnnCollector finalCollector; // holds every part's hits

    public IVFSegmentScan(int parts) {
        this.parts = parts;
    }

    /** How many parts share the scan. That many posting lists are prefetched ahead, one for each part to take next. */
    public int parts() {
        return parts;
    }

    /** The query quantized per centroid, shared so that each centroid's is computed once. */
    public ConcurrentMap<Integer, QuantizedQuery> quantizedQueries() {
        return quantizedQueries;
    }

    /**
     * The search's plan, made by the first search to ask for it: the prepare step, before the parts start. Null if the
     * segment has nothing to search. Parts that start together without that step each plan, and all use the plan set first.
     */
    Plan plan(IOSupplier<Plan> planner) throws IOException {
        Plan plan = this.plan.get();
        if (plan == null) {
            Plan planned = planner.get();
            this.plan.compareAndSet(null, planned == null ? NOTHING_TO_SEARCH : planned);
            plan = this.plan.get();
        }
        return plan == NOTHING_TO_SEARCH ? null : plan;
    }

    /** After the search: the collector holding the segment's hits, or null if no part visited anything. */
    public KnnCollector finalCollector() {
        return finalCollector;
    }

    /** After the search: how many vectors the parts visited in total. */
    public long visitedCount() {
        long total = 0;
        for (KnnCollector collector : collectors) {
            total += collector.visitedCount();
        }
        return total;
    }

    /** Runs {@code search} so that it plans the segment but takes no posting list. Call before the parts start. */
    public <E extends Exception> void prepare(CheckedRunnable<E> search) throws E {
        preparing = true;
        try {
            search.run();
        } finally {
            preparing = false;
        }
    }

    /**
     * Searches the planned segment as one part: takes posting lists within the budget, and may be the part that goes on past it.
     * @param postingLists the part's own view of the posting lists, to prefetch through: not the one its visitor reads
     */
    void search(PostingVisitor visitor, IndexInput postingLists, KnnCollector collector) throws IOException {
        if (preparing) {
            return;
        }
        Plan plan = this.plan.get();
        collectors.add(collector);
        while (takeNext(plan, visitor, postingLists)) {
            int scoredDocs = visit(visitor, collector);
            // Recorded last, the scored docs first: once every list is recorded, another part may take this collector's hits.
            scored.addAndGet(scoredDocs);
            visited.incrementAndGet();
        }
        // Only one part goes on past the budget: the first here once every posting list taken was visited.
        Taken end = taken.get();
        if (visited.get() < end.lists() || pastBudget.compareAndSet(false, true) == false) {
            return;
        }
        gatherHitsInto(collector);
        // alone from here, so one posting list ahead is prefetched, as in a search on its own
        CentroidIterator postings = new PrefetchingCentroidIterator(after(plan, end.lists()), postingLists);
        searchFrom(postings, plan, visitor, collector, end.entries(), scored.get());
    }

    /**
     * Takes the next posting list within the budget and moves the visitor onto it. False if none is left. A list's size is
     * only known once a visitor is on it, and whether the next list is within the budget depends on it: so a part moves onto
     * the next list in line, and takes it only if no other part did meanwhile.
     */
    private boolean takeNext(Plan plan, PostingVisitor visitor, IndexInput postingLists) throws IOException {
        PostingMetadata[] order = plan.order();
        while (true) {
            Taken before = taken.get();
            if (before.lists() == order.length || before.entries() >= plan.budget()) {
                return false;
            }
            long entries = visitor.resetPostingsScorer(order[before.lists()]);
            if (taken.compareAndSet(before, new Taken(before.lists() + 1, before.entries() + entries))) {
                // one list per part ahead, so that a list is asked for shortly before it is scored
                int ahead = before.lists() + parts;
                if (ahead < order.length) {
                    postingLists.prefetch(order[ahead].offset(), order[ahead].length());
                }
                return true;
            }
        }
    }

    /** The posting lists after the first {@code from} of the plan's order: the rest of the order, then the plan's rest. */
    private static CentroidIterator after(Plan plan, int from) {
        return new CentroidIterator() {
            private int next = from;

            @Override
            public boolean hasNext() {
                return next < plan.order().length || plan.rest().hasNext();
            }

            @Override
            public PostingMetadata nextPosting() throws IOException {
                return next < plan.order().length ? plan.order()[next++] : plan.rest().nextPosting();
            }
        };
    }

    /**
     * Searches a segment as one thread does: on along {@code postings}, from {@code budgetUsed} entries visited and
     * {@code scored} docs scored.
     */
    static void searchFrom(
        CentroidIterator postings,
        Plan plan,
        PostingVisitor visitor,
        KnnCollector collector,
        long budgetUsed,
        long scored
    ) throws IOException {
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

    /** Moves every other part's hits into {@code collector}, which can then tell whether the search found enough. */
    private void gatherHitsInto(KnnCollector collector) {
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
