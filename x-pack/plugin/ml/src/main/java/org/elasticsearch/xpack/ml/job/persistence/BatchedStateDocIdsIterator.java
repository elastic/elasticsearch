/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ml.utils.persistence.BatchedDocumentsIterator;

/**
 * Iterates through the state doc ids
 */
public class BatchedStateDocIdsIterator extends BatchedDocumentsIterator<String> {

    public BatchedStateDocIdsIterator(OriginSettingClient client, String index) {
        super(client, index);
    }

    @Override
    protected boolean shouldFetchSource() {
        return false;
    }

    @Override
    protected QueryBuilder getQuery() {
        return QueryBuilders.matchAllQuery();
    }

    @Override
    protected String map(SearchHit hit) {
        return hit.getId();
    }
}
