/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Objects;

/**
 * This is a document iterator that returns just the id of each matched document.
 */
public class DocIdBatchedDocumentIterator extends BatchedDocumentsIterator<String> {

    private final QueryBuilder query;

    public DocIdBatchedDocumentIterator(OriginSettingClient client, String index, QueryBuilder query) {
        super(client, index);
        this.query = Objects.requireNonNull(query);
    }

    @Override
    protected QueryBuilder getQuery() {
        return query;
    }

    @Override
    protected String map(SearchHit hit) {
        return hit.getId();
    }

    @Override
    protected boolean shouldFetchSource() {
        return false;
    }
}
