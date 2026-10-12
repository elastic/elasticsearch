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
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.PostingVisitor;

import java.io.IOException;

/**
 * Decides which posting lists one segment's IVF search visits: the centroid iterator's best-first order, cut by the visit
 * budget, then extended until the collector is full and, with filters or deletes, until enough docs were scored.
 * <p>
 * The decision lives here, apart from the visitors that score postings, so that several threads can later search parts
 * of one segment and still visit exactly the postings a single-threaded search visits.
 */
public final class IVFSegmentScan {

    /** What a search must know about a segment before it visits any posting. */
    record Plan(CentroidIterator postings, long budget, int numVectors, float percentFiltered, Bits acceptBits) {}

    private enum Phase {
        BUDGET,
        CATCH_UP,
        DONE
    }

    private Plan plan;
    private boolean empty;
    private Phase phase = Phase.BUDGET;
    private long charged;
    private long scored;
    private float expectedScored;

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

    /** Moves {@code visitor} onto the next posting list if the search should still visit one; false when it is done. */
    synchronized boolean claim(PostingVisitor visitor, KnnCollector collector) throws IOException {
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        if (phase == Phase.BUDGET && (plan.postings().hasNext() == false || (charged >= plan.budget() && isFull(collector)))) {
            startCatchUp();
        }
        if (phase == Phase.CATCH_UP && (plan.postings().hasNext() == false || (scored >= expectedScored && scored >= collector.k()))) {
            phase = Phase.DONE;
        }
        if (phase == Phase.DONE) {
            return false;
        }
        int size = visitor.resetPostingsScorer(plan.postings().nextPosting());
        if (phase == Phase.BUDGET) {
            charged += size;
        }
        return true;
    }

    /** Records a visited posting and how many docs its visit scored. */
    synchronized void complete(int scoredDocs) {
        scored += scoredDocs;
    }

    private static boolean isFull(KnnCollector collector) {
        return collector.minCompetitiveSimilarity() != Float.NEGATIVE_INFINITY;
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
