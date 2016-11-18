/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.Date;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A class that removes results from all the jobs that
 * have expired their respected retention time.
 */
public class OldDataRemover {

    private final JobProvider jobProvider;
    private final Function<String, ElasticsearchBulkDeleter> dataDeleterFactory;

    public OldDataRemover(JobProvider jobProvider, Function<String, ElasticsearchBulkDeleter> dataDeleterFactory) {
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.dataDeleterFactory = Objects.requireNonNull(dataDeleterFactory);
    }

    /**
     * Removes results between the time given and the current time
     */
    public void deleteResultsAfter(ActionListener<BulkResponse> listener, String jobId, long cutoffEpochMs) {
        Date now = new Date();
        JobDataDeleter deleter = dataDeleterFactory.apply(jobId);
        deleteResultsWithinRange(jobId, deleter, cutoffEpochMs, now.getTime());
        deleter.commit(listener);
    }

    private void deleteResultsWithinRange(String jobId, JobDataDeleter deleter, long start, long end) {
        deleteBatchedData(
                jobProvider.newBatchedInfluencersIterator(jobId).timeRange(start, end),
                deleter::deleteInfluencer
                );
        deleteBatchedData(
                jobProvider.newBatchedBucketsIterator(jobId).timeRange(start, end),
                deleter::deleteBucket
                );
    }

    private <T> void deleteBatchedData(BatchedDocumentsIterator<T> resultsIterator,
            Consumer<T> deleteFunction) {
        while (resultsIterator.hasNext()) {
            Deque<T> batch = resultsIterator.next();
            if (batch.isEmpty()) {
                return;
            }
            for (T result : batch) {
                deleteFunction.accept(result);
            }
        }
    }

}
