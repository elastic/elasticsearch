/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Iterator;
import java.util.function.BooleanSupplier;

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
    private final TaskId parentTaskId;

    AbstractExpiredJobDataRemover(OriginSettingClient client, Iterator<Job> jobIterator, TaskId parentTaskId) {
        this.client = client;
        this.jobIterator = jobIterator;
        this.parentTaskId = parentTaskId;
    }

    protected TaskId getParentTaskId() {
        return parentTaskId;
    }

    @Override
    public void remove(float requestsPerSecond, ActionListener<Boolean> listener, BooleanSupplier isTimedOutSupplier) {
        removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier);
    }

    private void removeData(
        Iterator<Job> jobIterator,
        float requestsPerSecond,
        ActionListener<Boolean> listener,
        BooleanSupplier isTimedOutSupplier
    ) {
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

        if (isTimedOutSupplier.getAsBoolean()) {
            listener.onResponse(false);
            return;
        }

        Long retentionDays = getRetentionDays(job);
        if (retentionDays == null) {
            removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier);
            return;
        }

        calcCutoffEpochMs(job.getId(), retentionDays, ActionListener.wrap(response -> {
            if (response == null) {
                removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier);
            } else {
                removeDataBefore(
                    job,
                    requestsPerSecond,
                    response.latestTimeMs,
                    response.cutoffEpochMs,
                    ActionListener.wrap(r -> removeData(jobIterator, requestsPerSecond, listener, isTimedOutSupplier), listener::onFailure)
                );
            }
        }, listener::onFailure));
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

    /**
     * The latest time that cutoffs are measured from is not wall clock time,
     * but some other reference point that makes sense for the type of data
     * being removed.  This class groups the cutoff time with it's "latest"
     * reference point.
     */
    protected record CutoffDetails(long latestTimeMs, long cutoffEpochMs) {}
}
