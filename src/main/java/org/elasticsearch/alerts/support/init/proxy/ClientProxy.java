/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.init.proxy;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.alerts.support.init.InitializingService;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A lazily initialized proxy to an elasticsearch {@link Client}. Inject this proxy whenever a client
 * needs to injected to be avoid circular dependencies issues.
 */
public class ClientProxy implements InitializingService.Initializable {

    private Client client;

    /**
     * Creates a proxy to the given client (can be used for testing)
     */
    public static ClientProxy of(Client client) {
        ClientProxy proxy = new ClientProxy();
        proxy.client = client;
        return proxy;
    }

    @Override
    public void init(Injector injector) {
        client = injector.getInstance(Client.class);
    }

    public AdminClient admin() {
        return client.admin();
    }

    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return client.index(request);
    }

    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        client.index(request, listener);
    }

    public IndexRequestBuilder prepareIndex(String index, String type, String id) {
        return client.prepareIndex(index, type, id);
    }

    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return client.delete(request);
    }

    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return client.prepareGet(index, type, id);
    }

    public SearchResponse search(SearchRequest request) {
        return client.search(request).actionGet();
    }

    public SearchResponse searchScroll(String scrollId, TimeValue timeout) {
        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(timeout);
        return client.searchScroll(request).actionGet();
    }

    public ClearScrollResponse clearScroll(String scrollId) {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        return client.clearScroll(request).actionGet();
    }

    public RefreshResponse refresh(RefreshRequest request) {
        return client.admin().indices().refresh(request).actionGet();
    }

}
