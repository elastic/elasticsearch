/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Removes job data that expired with respect to their retention period.
 *
 * <p> The implementation ensures removal happens in asynchronously to avoid
 * blocking the thread it was called at for too long. It does so by
 * chaining the steps together.
 */
abstract class AbstractExpiredJobDataRemover implements MlDataRemover {

    private final ClusterService clusterService;

    AbstractExpiredJobDataRemover(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    public void remove(ActionListener<Boolean> listener) {
        removeData(newJobIterator(), listener);
    }

    private void removeData(Iterator<Job> jobIterator, ActionListener<Boolean> listener) {
        if (jobIterator.hasNext() == false) {
            listener.onResponse(true);
            return;
        }
        Job job = jobIterator.next();
        Long retentionDays = getRetentionDays(job);
        if (retentionDays == null) {
            removeData(jobIterator, listener);
            return;
        }
        long cutoffEpochMs = calcCutoffEpochMs(retentionDays);
        removeDataBefore(job, cutoffEpochMs, ActionListener.wrap(response -> removeData(jobIterator, listener), listener::onFailure));
    }

    private Iterator<Job> newJobIterator() {
        ClusterState clusterState = clusterService.state();
        List<Job> jobs = new ArrayList<>(MlMetadata.getMlMetadata(clusterState).getJobs().values());
        return createVolatileCursorIterator(jobs);
    }

    protected static <T> Iterator<T> createVolatileCursorIterator(List<T> items) {
        return new VolatileCursorIterator<T>(items);
    }

    private long calcCutoffEpochMs(long retentionDays) {
        long nowEpochMs = DateTime.now(ISOChronology.getInstance()).getMillis();
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
}
