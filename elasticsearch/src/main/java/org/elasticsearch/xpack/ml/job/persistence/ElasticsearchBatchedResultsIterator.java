/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.ml.job.results.Result;

abstract class ElasticsearchBatchedResultsIterator<T> extends ElasticsearchBatchedDocumentsIterator<T> {

    public ElasticsearchBatchedResultsIterator(Client client, String jobId, String resultType) {
        super(client, AnomalyDetectorsIndex.jobResultsIndexName(jobId),
                new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), resultType));
    }

    @Override
    protected String getType() {
        return Result.TYPE.getPreferredName();
    }
}
