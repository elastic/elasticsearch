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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.PostingVisitor;

import java.io.IOException;

/**
 * Decides which posting lists one segment's IVF search visits: the centroid iterator's best-first order, cut by the visit
 * budget, then extended until the collector is full and, with filters or deletes, until enough docs were scored.
 * <p>
 * The decision lives here, apart from the visitors that score postings, so that several threads ("parts") can search one
 * segment through a shared scan and still visit exactly the postings a single-threaded search visits. Each part has its
 * own visitor and collector; the scan hands out postings in order and charges each one's size when it is claimed. A part
 * waits only when the next decision depends on postings other parts are still visiting, and never for a part to start.
 */
public final class IVFSegmentScan {

    /** What a search must know about a segment before it visits any posting. */
    record Plan(CentroidIterator postings, long budget, int numVectors, float percentFiltered, Bits acceptBits) {}

    private enum Phase {
        BUDGET,
        CATCH_UP,
        DONE
    }

    private enum Decision {
        CLAIM,
        WAIT,
        STOP
    }

    private final boolean shared;
    private Plan plan;
    private boolean empty;
    private boolean wholeClaimed;
    private boolean failed;
    private Phase phase = Phase.BUDGET;
    private long charged;
    private long scored;
    private int inFlight;
    private long inFlightSize;
    private float expectedScored;

    /** A scan shared by the parts of one segment's search; parts must use collectors of the same size. */
    public IVFSegmentScan() {
        this(true);
    }

    private IVFSegmentScan(boolean shared) {
        this.shared = shared;
    }

    /** A scan used by a single search on its own; it reads "collector full" from the collector itself. */
    static IVFSegmentScan single() {
        return new IVFSegmentScan(false);
    }

    /** Builds the plan with {@code planner} unless already open. Returns false if the segment has nothing to search. */
    synchronized boolean openIfNeeded(IOSupplier<Plan> planner) throws IOException {
        if (plan == null && empty == false) {
            plan = planner.get();
            empty = plan == null;
        }
        return empty == false;
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
     * Moves {@code visitor} onto the next posting list if the search should still visit one. Returns the posting's size,
     * to pass back to {@link #complete}, or -1 when the search is done.
     */
    synchronized int claim(PostingVisitor visitor, KnnCollector collector) throws IOException {
        Decision decision;
        while ((decision = decide(collector)) == Decision.WAIT) {
            try {
                wait();
            } catch (InterruptedException e) {
                fail();
                throw new ThreadInterruptedException(e);
            }
        }
        if (decision == Decision.STOP) {
            return -1;
        }
        final int size;
        try {
            size = visitor.resetPostingsScorer(plan.postings().nextPosting());
        } catch (IOException | RuntimeException e) {
            fail();
            throw e;
        }
        if (phase == Phase.BUDGET) {
            charged += size;
        }
        inFlight++;
        inFlightSize += size;
        return size;
    }

    /** Records that a claimed posting of {@code size} entries was visited and scored {@code scoredDocs} docs. */
    synchronized void complete(int size, int scoredDocs) {
        inFlight--;
        inFlightSize -= size;
        scored += scoredDocs;
        notifyAll();
    }

    /** Stops every part: a part failed, so the search fails and the others should not keep scanning. */
    synchronized void fail() {
        failed = true;
        notifyAll();
    }

    /**
     * The search loop's conditions, evaluated on the scan's counters. While other parts are visiting postings, the number
     * of docs scored so far is only known within {@code [scored, scored + inFlightSize]}; wait if that range straddles a
     * threshold.
     */
    private Decision decide(KnnCollector collector) {
        if (failed) {
            return Decision.STOP;
        }
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        if (phase == Phase.BUDGET) {
            if (plan.postings().hasNext() && charged < plan.budget()) {
                return Decision.CLAIM;
            }
            Decision fill = plan.postings().hasNext() ? scoredBelow(collector.k(), collector) : Decision.STOP;
            if (fill != Decision.STOP) {
                return fill;
            }
            startCatchUp();
        }
        if (phase == Phase.CATCH_UP) {
            if (plan.postings().hasNext() == false) {
                phase = Phase.DONE;
                return Decision.STOP;
            }
            Decision more = scoredBelow(Math.max(expectedScored, collector.k()), null);
            if (more == Decision.STOP) {
                phase = Phase.DONE;
            }
            return more;
        }
        return Decision.STOP;
    }

    /**
     * CLAIM if fewer than {@code threshold} docs were scored, STOP if at least that many were, WAIT if postings in flight
     * decide it. A single search's fill check asks its {@code collector} instead, which may count differently (e.g. by
     * parent for nested docs).
     */
    private Decision scoredBelow(float threshold, KnnCollector collector) {
        if (shared == false && collector != null) {
            return collector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY ? Decision.CLAIM : Decision.STOP;
        }
        if (scored >= threshold) {
            return Decision.STOP;
        }
        return scored + inFlightSize < threshold ? Decision.CLAIM : Decision.WAIT;
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
