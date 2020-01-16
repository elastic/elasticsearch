/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.job.persistence.BatchedJobsIterator;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;

import java.time.Clock;
import java.time.Instant;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Removes job data that expired with respect to their retention period.
 *
 * <p> The implementation ensures removal happens in asynchronously to avoid
 * blocking the thread it was called at for too long. It does so by
 * chaining the steps together.
 */
abstract class AbstractExpiredJobDataRemover implements MlDataRemover {

    private final OriginSettingClient client;

    AbstractExpiredJobDataRemover(OriginSettingClient client) {
        this.client = client;
    }

    @Override
    public void remove(ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        removeData(newJobIterator(), listener, isTimedOutSupplier);
    }

    private void removeData(WrappedBatchedJobsIterator jobIterator, ActionListener<Boolean> listener,
                            Supplier<Boolean> isTimedOutSupplier) {
        if (jobIterator.hasNext() == false) {
            listener.onResponse(true);
            return;
        }
        Job job = jobIterator.next();
        if (job == null) {
            // maybe null if the batched iterator search return no results
            listener.onResponse(true);
            return;
        }

        if (isTimedOutSupplier.get()) {
            listener.onResponse(false);
            return;
        }

        Long retentionDays = getRetentionDays(job);
        if (retentionDays == null) {
            removeData(jobIterator, listener, isTimedOutSupplier);
            return;
        }
        long cutoffEpochMs = calcCutoffEpochMs(retentionDays);
        removeDataBefore(job, cutoffEpochMs,
            ActionListener.wrap(response -> removeData(jobIterator, listener, isTimedOutSupplier), listener::onFailure));
    }

    private WrappedBatchedJobsIterator newJobIterator() {
        BatchedJobsIterator jobsIterator = new BatchedJobsIterator(client, AnomalyDetectorsIndex.configIndexName());
        return new WrappedBatchedJobsIterator(jobsIterator);
    }

    private long calcCutoffEpochMs(long retentionDays) {
        long nowEpochMs = Instant.now(Clock.systemDefaultZone()).toEpochMilli();
        return nowEpochMs - new TimeValue(retentionDays, TimeUnit.DAYS).getMillis();
    }

    protected abstract Long getRetentionDays(Job job);

    /**
     * Template method to allow implementation details of various types of data (e.g. results, model snapshots).
     * Implementors need to call {@code listener.onResponse} when they are done in order to continue to the next job.
     */
    protected abstract void removeDataBefore(Job job, long cutoffEpochMs, ActionListener<Boolean> listener);

    protected static BoolQueryBuilder createQuery(String jobId, long cutoffEpochMs) {
        return QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).lt(cutoffEpochMs).format("epoch_millis"));
    }

    /**
     * BatchedJobsIterator efficiently returns batches of jobs using a scroll
     * search but AbstractExpiredJobDataRemover works with one job at a time.
     * This class abstracts away the logic of pulling one job at a time from
     * multiple batches.
     */
    private class WrappedBatchedJobsIterator implements Iterator<Job> {
        private final BatchedJobsIterator batchedIterator;
        private VolatileCursorIterator<Job> currentBatch;

        WrappedBatchedJobsIterator(BatchedJobsIterator batchedIterator) {
            this.batchedIterator = batchedIterator;
        }

        @Override
        public boolean hasNext() {
            return (currentBatch != null && currentBatch.hasNext()) || batchedIterator.hasNext();
        }

        /**
         * Before BatchedJobsIterator has run a search it reports hasNext == true
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
}
