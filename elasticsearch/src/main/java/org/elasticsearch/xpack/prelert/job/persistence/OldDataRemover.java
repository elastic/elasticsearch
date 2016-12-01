/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.Objects;
import java.util.function.Function;

/**
 * A class that removes results from all the jobs that
 * have expired their respected retention time.
 */
public class OldDataRemover {

    private final Function<String, ElasticsearchBulkDeleter> dataDeleterFactory;

    public OldDataRemover(Function<String, ElasticsearchBulkDeleter> dataDeleterFactory) {
        this.dataDeleterFactory = Objects.requireNonNull(dataDeleterFactory);
    }

    /**
     * Removes results between the time given and the current time
     */
    public void deleteResultsAfter(ActionListener<BulkResponse> listener, String jobId, long cutoffEpochMs) {
        JobDataDeleter deleter = dataDeleterFactory.apply(jobId);
        deleter.deleteResultsFromTime(cutoffEpochMs);
        deleter.commit(listener);
    }
}
