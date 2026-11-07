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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.PostingVisitor;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.QuantizedQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Decides which posting lists one segment's IVF search visits: the centroid iterator's best-first order, cut by the visit
 * budget, then extended until the collector is full and, with filters or deletes, until enough docs were scored.
 * <p>
 * The decision lives here, apart from the visitors that score postings, so that several threads ("parts") can search one
 * segment through a shared scan and still visit exactly the postings a single-threaded search visits. Each part has its
 * own visitor and collector. A shared scan lists the postings within its budget once, when the first part arrives, and
 * parts then claim them by index without a lock. What follows the budget depends on how many docs were scored, so the
 * part that finishes the last listed posting continues alone (the "tail"). No part ever waits for another to finish.
 */
public final class IVFSegmentScan {

    /** What a search must know about a segment before it visits any posting. */
    record Plan(CentroidIterator postings, long budget, int numVectors, float percentFiltered, Bits acceptBits) {}

    private enum Phase {
        BUDGET,
        CATCH_UP,
        DONE
    }

    /** The ticket of a posting claimed past the listed budget, or by a search on its own. */
    private static final int UNLISTED = Integer.MAX_VALUE;

    private final boolean shared;
    private final int parts;
    private Plan plan;
    private boolean empty;
    private boolean wholeClaimed;
    private volatile boolean failed;
    // the sequential loop: a search on its own, or a shared scan's listing and tail; guarded by this
    private Phase phase = Phase.BUDGET;
    private long charged;
    private float expectedScored;
    private final AtomicLong scored = new AtomicLong();
    // a shared scan's budget, listed once
    private volatile PostingMetadata[] listed;
    private boolean paused; // guarded by this
    private volatile int claimLimit; // parts claim listed postings below this index
    private final AtomicInteger nextListed = new AtomicInteger();
    private final AtomicInteger completedListed = new AtomicInteger();
    private final AtomicReference<PostingVisitor> tail = new AtomicReference<>();
    private final ConcurrentMap<Integer, QuantizedQuery> quantizedQueries = new ConcurrentHashMap<>();

    /**
     * A scan shared by {@code parts} searches of one segment (sizing how far ahead posting lists are prefetched); parts must
     * use collectors of the same size.
     */
    public IVFSegmentScan(int parts) {
        this(true, parts);
    }

    private IVFSegmentScan(boolean shared, int parts) {
        if (parts < 1) {
            throw new IllegalArgumentException("parts must be at least 1, got: " + parts);
        }
        this.shared = shared;
        this.parts = parts;
    }

    /** A scan used by a single search on its own; it reads "collector full" from the collector itself. */
    static IVFSegmentScan single() {
        return new IVFSegmentScan(false, 1);
    }

    int parts() {
        return parts;
    }

    /** The query quantized per centroid by the parts' visitors, shared so that each centroid's is computed once. */
    public ConcurrentMap<Integer, QuantizedQuery> quantizedQueries() {
        return quantizedQueries;
    }

    /** Builds the plan with {@code planner} unless already open. Returns false if the segment has nothing to search. */
    synchronized boolean openIfNeeded(IOSupplier<Plan> planner) throws IOException {
        if (plan == null && empty == false && failed == false) {
            try {
                plan = planner.get();
            } catch (IOException | RuntimeException e) {
                fail();
                throw e;
            }
            empty = plan == null;
        }
        return empty == false && failed == false;
    }

    /**
     * True once no part starting now could claim a posting list, so it need not prepare to visit any: the search is over, or
     * every posting it may claim is taken and the tail is not its to take (a listed posting is unfinished, or paused).
     */
    boolean isDone() {
        PostingMetadata[] postings = listed;
        boolean nothingToClaim = postings != null && nextListed.get() >= claimLimit && completedListed.get() < postings.length;
        return failed || tail.get() != null || nothingToClaim;
    }

    /** The docs the search may score, shared read-only by every visitor of this segment. */
    synchronized Bits acceptBits() {
        return plan.acceptBits();
    }

    /** For segments searched without postings: true for exactly one part, which then searches the whole segment. */
    synchronized boolean claimWhole() {
        boolean first = wholeClaimed == false;
        wholeClaimed = true;
        return first;
    }

    /**
     * Moves {@code visitor} onto the next posting list if the search should still visit one. Returns a ticket to pass back
     * to {@link #complete}, or -1 when the search is done or paused.
     */
    int claim(PostingVisitor visitor, KnnCollector collector) throws IOException {
        if (failed) {
            return -1;
        }
        if (shared == false) {
            return claimNext(visitor, collector);
        }
        PostingMetadata[] postings = listed != null ? listed : list(visitor);
        if (postings == null) {
            return -1; // listing failed
        }
        int i;
        do {
            i = nextListed.get();
            if (i >= claimLimit) {
                return claimTail(visitor, collector);
            }
        } while (nextListed.compareAndSet(i, i + 1) == false);
        reset(visitor, postings[i]);
        return i;
    }

    /** Records that the posting of {@code ticket} was visited, scoring {@code scoredDocs} docs. */
    void complete(int ticket, int scoredDocs) {
        scored.addAndGet(scoredDocs);
        if (ticket != UNLISTED) {
            completedListed.incrementAndGet();
        }
    }

    /**
     * Lets the first part list the budget without claiming any of it, until {@link #resume}, so that the other parts start
     * with work instead of waiting for the plan. Must not be called while parts run.
     */
    public synchronized void pause() {
        paused = true;
        if (listed != null) {
            claimLimit = 0;
        }
    }

    public synchronized void resume() {
        paused = false;
        if (listed != null) {
            claimLimit = listed.length;
        }
    }

    /** Stops every part: a part failed, so the search fails and the others should not keep scanning. */
    void fail() {
        failed = true;
    }

    /** Lists the postings within the budget, in order and charging their sizes, so that parts can claim them by index. */
    private synchronized PostingMetadata[] list(PostingVisitor visitor) throws IOException {
        if (listed == null && failed == false) {
            PostingMetadata[] postings = new PostingMetadata[16];
            int count = 0;
            while (plan.postings().hasNext() && charged < plan.budget()) {
                PostingMetadata posting = next();
                if (count == postings.length) {
                    postings = ArrayUtil.growExact(postings, count * 2);
                }
                postings[count++] = posting;
                charged += reset(visitor, posting);
            }
            claimLimit = paused ? 0 : count;
            listed = Arrays.copyOf(postings, count); // published last: parts read claimLimit after reading it
        }
        return listed;
    }

    /**
     * Past the parts' share of the listed budget. Once every listed posting is finished, what comes next depends only on how
     * many docs they scored: the first part to get here continues alone.
     */
    private int claimTail(PostingVisitor visitor, KnnCollector collector) throws IOException {
        boolean owner = tail.get() == visitor || (completedListed.get() == listed.length && tail.compareAndSet(null, visitor));
        return owner ? claimNext(visitor, collector) : -1;
    }

    /** The sequential loop's next posting, if its conditions say to visit one. */
    private synchronized int claimNext(PostingVisitor visitor, KnnCollector collector) throws IOException {
        if (failed || decide(collector) == false) {
            return -1;
        }
        int size = reset(visitor, next());
        if (phase == Phase.BUDGET) {
            charged += size;
        }
        return UNLISTED;
    }

    private PostingMetadata next() throws IOException {
        try {
            return plan.postings().nextPosting();
        } catch (IOException | RuntimeException e) {
            fail();
            throw e;
        }
    }

    private int reset(PostingVisitor visitor, PostingMetadata posting) throws IOException {
        try {
            return visitor.resetPostingsScorer(posting);
        } catch (IOException | RuntimeException e) {
            fail();
            throw e;
        }
    }

    /** The search loop's conditions, evaluated on the scan's counters: whether to visit another posting. */
    private boolean decide(KnnCollector collector) {
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        if (phase == Phase.BUDGET) {
            if (plan.postings().hasNext() && (charged < plan.budget() || scoredBelow(collector.k(), collector))) {
                return true;
            }
            startCatchUp();
        }
        if (phase == Phase.CATCH_UP) {
            if (plan.postings().hasNext() && scoredBelow(Math.max(expectedScored, collector.k()), null)) {
                return true;
            }
            phase = Phase.DONE;
        }
        return false;
    }

    /**
     * Whether fewer than {@code threshold} docs were scored. A single search's fill check asks its {@code collector}
     * instead, which may count differently (e.g. by parent for nested docs).
     */
    private boolean scoredBelow(float threshold, KnnCollector collector) {
        if (shared == false && collector != null) {
            return collector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY;
        }
        return scored.get() < threshold;
    }

    /** Filters and deletes skip docs, so keep going until about as many docs were scored as an unfiltered search scores. */
    private void startCatchUp() {
        if (plan.acceptBits() == null) {
            phase = Phase.DONE;
            return;
        }
        // TODO Adjust the value here when using centroid filtering
        float unfilteredRatioVisited = (float) charged / plan.numVectors();
        int filteredVectors = (int) Math.ceil(plan.numVectors() * plan.percentFiltered());
        expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, charged / 2f);
        phase = Phase.CATCH_UP;
    }
}
