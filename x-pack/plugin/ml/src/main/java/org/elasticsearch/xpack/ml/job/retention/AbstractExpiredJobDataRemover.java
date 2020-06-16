/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Removes job data that expired with respect to their retention period.
 *
 * <p> The implementation ensures removal happens in asynchronously to avoid
 * blocking the thread it was called at for too long. It does so by
 * chaining the steps together.
 */
abstract class AbstractExpiredJobDataRemover implements MlDataRemover {

    protected final OriginSettingClient client;
    private final Iterator<Job> jobIterator;

    AbstractExpiredJobDataRemover(OriginSettingClient client, Iterator<Job> jobIterator) {
        this.client = client;
        this.jobIterator = jobIterator;
    }

    @Override
    public void remove(float requestsPerSecond,
                       ActionListener<Boolean> listener,
                       Supplier<Boolean> isTimedOutSupplier) {
        removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier);
    }

    private void removeData(Iterator<Job> jobIterator,
                            float requestsPerSecond,
                            ActionListener<Boolean> listener,
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
            removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier);
            return;
        }

        calcCutoffEpochMs(job.getId(), retentionDays, ActionListener.wrap(
                response -> {
                    if (response == null) {
                        removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier);
                    } else {
                        removeDataBefore(job, requestsPerSecond, response.latestTimeMs, response.cutoffEpochMs, ActionListener.wrap(
                                r -> removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier),
                                listener::onFailure));
                    }
                },
                listener::onFailure
        ));
    }

    abstract void calcCutoffEpochMs(String jobId, long retentionDays, ActionListener<CutoffDetails> listener);

    abstract Long getRetentionDays(Job job);

    /**
     * Template method to allow implementation details of various types of data (e.g. results, model snapshots).
     * Implementors need to call {@code listener.onResponse} when they are done in order to continue to the next job.
     */
    abstract void removeDataBefore(
        Job job,
        float requestsPerSecond,
        long latestTimeMs,
        long cutoffEpochMs,
        ActionListener<Boolean> listener
    );

    static BoolQueryBuilder createQuery(String jobId, long cutoffEpochMs) {
        return QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).lt(cutoffEpochMs).format("epoch_millis"));
    }

    /**
     * The latest time that cutoffs are measured from is not wall clock time,
     * but some other reference point that makes sense for the type of data
     * being removed.  This class groups the cutoff time with it's "latest"
     * reference point.
     */
    protected static final class CutoffDetails {

        public final long latestTimeMs;
        public final long cutoffEpochMs;

        public CutoffDetails(long latestTimeMs, long cutoffEpochMs) {
            this.latestTimeMs = latestTimeMs;
            this.cutoffEpochMs = cutoffEpochMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(latestTimeMs, cutoffEpochMs);
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other instanceof CutoffDetails == false) {
                return false;
            }
            CutoffDetails that = (CutoffDetails) other;
            return this.latestTimeMs == that.latestTimeMs &&
                this.cutoffEpochMs == that.cutoffEpochMs;
        }
    }
}
