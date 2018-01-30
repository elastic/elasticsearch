/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Result;

public abstract class BatchedResultsIterator<T> extends BatchedDocumentsIterator<Result<T>> {

    private final ResultsFilterBuilder filterBuilder;

    public BatchedResultsIterator(Client client, String jobId, String resultType) {
        super(client, AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        this.filterBuilder = new ResultsFilterBuilder(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), resultType));
    }

    /**
     * Query documents whose timestamp is within the given time range
     *
     * @param startEpochMs the start time as epoch milliseconds (inclusive)
     * @param endEpochMs the end time as epoch milliseconds (exclusive)
     * @return the iterator itself
     */
    public BatchedResultsIterator<T> timeRange(long startEpochMs, long endEpochMs) {
        filterBuilder.timeRange(Result.TIMESTAMP.getPreferredName(), startEpochMs, endEpochMs);
        return this;
    }

    /**
     * Sets whether interim results should be included
     *
     * @param includeInterim Whether interim results should be included
     */
    public BatchedResultsIterator<T> includeInterim(boolean includeInterim) {
        filterBuilder.interim(includeInterim);
        return this;
    }

    protected final QueryBuilder getQuery() {
        return filterBuilder.build();
    }
}
