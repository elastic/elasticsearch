/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;

import java.util.Objects;

public class ReindexRequestBuilder2 implements RequestBuilder<ReindexRequest> {

    private SearchRequest searchRequest;
    private IndexRequest destination;
    private RemoteInfo remoteInfo;

    public ReindexRequestBuilder2() {}

    public ReindexRequestBuilder2 search(SearchRequest search) {
        this.searchRequest = search;
        return this;
    }

    public ReindexRequestBuilder2 destination(IndexRequest destination) {
        this.destination = destination;
        return this;
    }

    public ReindexRequestBuilder2 remoteInfo(RemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
        return this;
    }

    @Override
    public ReindexRequest buildRequest() {
        SearchRequest finalSearchRequest = searchRequest == null ? new SearchRequest() : searchRequest;
        boolean mustCreateDestination = destination == null;
        IndexRequest finalDestination = Objects.requireNonNullElseGet(destination, IndexRequest::new);
        try {
            ReindexRequest reindexRequest = new ReindexRequest(finalSearchRequest, finalDestination);
            try {
                if (remoteInfo != null) {
                    reindexRequest.setRemoteInfo(remoteInfo);
                }
                return reindexRequest;
            } catch (Exception e) {
                reindexRequest.decRef();
                throw e;
            }
        } finally {
            if (mustCreateDestination) {
                finalDestination.decRef();
            }
        }
    }
}
