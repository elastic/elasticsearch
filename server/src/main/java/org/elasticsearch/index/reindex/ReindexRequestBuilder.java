/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class ReindexRequestBuilder extends
        AbstractBulkIndexByScrollRequestBuilder<ReindexRequest, ReindexRequestBuilder> {
    private final IndexRequestBuilder destination;

    public ReindexRequestBuilder(ElasticsearchClient client,
            ActionType<BulkByScrollResponse> action) {
        this(client, action, new SearchRequestBuilder(client, SearchAction.INSTANCE),
                new IndexRequestBuilder(client, IndexAction.INSTANCE));
    }

    private ReindexRequestBuilder(ElasticsearchClient client,
            ActionType<BulkByScrollResponse> action,
            SearchRequestBuilder search, IndexRequestBuilder destination) {
        super(client, action, search, new ReindexRequest(search.request(), destination.request()));
        this.destination = destination;
    }

    @Override
    protected ReindexRequestBuilder self() {
        return this;
    }

    public IndexRequestBuilder destination() {
        return destination;
    }

    /**
     * Set the destination index.
     */
    public ReindexRequestBuilder destination(String index) {
        destination.setIndex(index);
        return this;
    }

    /**
     * Setup reindexing from a remote cluster.
     */
    public ReindexRequestBuilder setRemoteInfo(RemoteInfo remoteInfo) {
        request().setRemoteInfo(remoteInfo);
        return this;
    }
}
