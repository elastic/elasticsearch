/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A wrapper around {@link BatchedIterator} that allows iterating jobs one
 * at a time from the batches returned by {@code BatchedIterator}
 *
 * This class abstracts away the logic of pulling one job at a time from
 * multiple batches.
 */
public class WrappedBatchedJobsIterator implements Iterator<Job> {
    private final BatchedIterator<Job.Builder> batchedIterator;
    private VolatileCursorIterator<Job> currentBatch;

    public WrappedBatchedJobsIterator(BatchedIterator<Job.Builder> batchedIterator) {
        this.batchedIterator = batchedIterator;
    }

    @Override
    public boolean hasNext() {
        return (currentBatch != null && currentBatch.hasNext()) || batchedIterator.hasNext();
    }

    /**
     * Before BatchedIterator has run a search it reports hasNext == true
     * but the first search may return no results. In that case null is return
     * and clients have to handle null.
     */
    @Override
    public Job next() {
        if (currentBatch != null && currentBatch.hasNext()) {
            return currentBatch.next();
        }

        // currentBatch is either null or all its elements have been iterated.
        // get the next currentBatch
        currentBatch = createBatchIteratorFromBatch(batchedIterator.next());

        // BatchedJobsIterator.hasNext maybe true if searching the first time
        // but no results are returned.
        return currentBatch.hasNext() ? currentBatch.next() : null;
    }

    private VolatileCursorIterator<Job> createBatchIteratorFromBatch(Deque<Job.Builder> builders) {
        List<Job> jobs = builders.stream().map(Job.Builder::build).collect(Collectors.toList());
        return new VolatileCursorIterator<>(jobs);
    }
}
