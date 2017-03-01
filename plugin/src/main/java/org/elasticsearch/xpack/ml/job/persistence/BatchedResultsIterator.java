/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.ml.job.results.Result;

public abstract class BatchedResultsIterator<T>
        extends BatchedDocumentsIterator<BatchedResultsIterator.ResultWithIndex<T>> {

    public BatchedResultsIterator(Client client, String jobId, String resultType) {
        super(client, AnomalyDetectorsIndex.jobResultsAliasedName(jobId),
                new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), resultType));
    }

    @Override
    protected String getType() {
        return Result.TYPE.getPreferredName();
    }

    public static class ResultWithIndex<T> {
        public final String indexName;
        public final T result;

        public ResultWithIndex(String indexName, T result) {
            this.indexName = indexName;
            this.result = result;
        }
    }
}
