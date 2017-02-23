/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.ml.MlDailyManagementService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Collections;
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
abstract class AbstractExpiredJobDataRemover implements MlDailyManagementService.Listener {

    private final ClusterService clusterService;

    AbstractExpiredJobDataRemover(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    public void onTrigger() {
        removeData(newJobIterator());
    }

    private Iterator<Job> newJobIterator() {
        List<Job> jobs = new ArrayList<>();
        ClusterState clusterState = clusterService.state();
        MlMetadata mlMetadata = clusterState.getMetaData().custom(MlMetadata.TYPE);
        if (mlMetadata != null) {
            jobs.addAll(mlMetadata.getJobs().values());
        }
        return createVolatileCursorIterator(jobs);
    }

    protected static <T> Iterator<T> createVolatileCursorIterator(List<T> items) {
        return new VolatileCursorIterator<T>(items);
    }

    protected void removeData(Iterator<Job> jobIterator) {
        if (jobIterator.hasNext() == false) {
            return;
        }
        Job job = jobIterator.next();
        Long retentionDays = getRetentionDays(job);
        if (retentionDays == null) {
            removeData(jobIterator);
            return;
        }
        long cutoffEpochMs = calcCutoffEpochMs(retentionDays);
        removeDataBefore(job, cutoffEpochMs, () -> removeData(jobIterator));
    }

    private long calcCutoffEpochMs(long retentionDays) {
        long startOfDayEpochMs = DateTime.now(ISOChronology.getInstance()).withTimeAtStartOfDay().getMillis();
        return startOfDayEpochMs - new TimeValue(retentionDays, TimeUnit.DAYS).getMillis();
    }

    protected abstract Long getRetentionDays(Job job);

    /**
     * Template method to allow implementation details of various types of data (e.g. results, model snapshots).
     * Implementors need to call {@code onFinish} when they are done in order to continue to the next job.
     */
    protected abstract void removeDataBefore(Job job, long cutoffEpochMs, Runnable onFinish);

    protected static BoolQueryBuilder createQuery(String jobId, long cutoffEpochMs) {
        return QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).lt(cutoffEpochMs).format("epoch_millis"));
    }

    private static class VolatileCursorIterator<T> implements Iterator<T> {
        private final List<T> items;
        private volatile int cursor;

        private VolatileCursorIterator(List<T> items) {
            this.items = items;
            this.cursor = 0;
        }

        @Override
        public boolean hasNext() {
            return cursor < items.size();
        }

        @Override
        public T next() {
            return items.get(cursor++);
        }
    }
}
