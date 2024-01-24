/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT;

public class ReindexRequestBuilder extends AbstractBulkIndexByScrollRequestBuilder<ReindexRequest, ReindexRequestBuilder> {
    private final IndexRequestBuilder destinationBuilder;
    private RemoteInfo remoteInfo;

    public ReindexRequestBuilder(ElasticsearchClient client) {
        this(client, new SearchRequestBuilder(client), new IndexRequestBuilder(client));
    }

    private ReindexRequestBuilder(ElasticsearchClient client, SearchRequestBuilder search, IndexRequestBuilder destination) {
        super(client, ReindexAction.INSTANCE, search);
        this.destinationBuilder = destination;
        initSourceSearchRequest();
    }

    /*
     * The following is normally done within the ReindexRequest constructor. But that constructor is not called until the request()
     * method is called once this builder is complete. Doing it there blows away changes made to the source request.
     */
    private void initSourceSearchRequest() {
        source().request().source(new SearchSourceBuilder());
        source().request().scroll(DEFAULT_SCROLL_TIMEOUT);
        source().request().source(new SearchSourceBuilder());
        source().request().source().size(DEFAULT_SCROLL_SIZE);
    }

    @Override
    protected ReindexRequestBuilder self() {
        return this;
    }

    public IndexRequestBuilder destination() {
        return destinationBuilder;
    }

    /**
     * Set the destination index.
     */
    public ReindexRequestBuilder destination(String index) {
        destinationBuilder.setIndex(index);
        return this;
    }

    /**
     * Setup reindexing from a remote cluster.
     */
    public ReindexRequestBuilder setRemoteInfo(RemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
        return this;
    }

    @Override
    public ReindexRequest request() {
        SearchRequest source = source().request();
        try {
            IndexRequest destination = destinationBuilder.request();
            try {
                ReindexRequest reindexRequest = new ReindexRequest(source, destination, false);
                try {
                    apply(reindexRequest);
                    return reindexRequest;
                } catch (Exception e) {
                    reindexRequest.decRef();
                    throw e;
                }
            } catch (Exception e) {
                destination.decRef();
                throw e;
            }
        } catch (Exception e) {
            source.decRef();
            throw e;
        }
    }

    @Override
    public void apply(ReindexRequest request) {
        super.apply(request);
        if (remoteInfo != null) {
            request.setRemoteInfo(remoteInfo);
        }
    }
}
