/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.init.proxy;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.xpack.common.init.proxy.ClientProxy;

/**
 * A lazily initialized proxy to an elasticsearch {@link Client}. Inject this proxy whenever a client
 * needs to injected to be avoid circular dependencies issues.
 */
public class WatcherClientProxy extends ClientProxy {

    private final TimeValue defaultSearchTimeout;
    private final TimeValue defaultIndexTimeout;
    private final TimeValue defaultBulkTimeout;

    @Inject
    public WatcherClientProxy(Settings settings) {
        defaultSearchTimeout = settings.getAsTime("xpack.watcher.internal.ops.search.default_timeout", TimeValue.timeValueSeconds(30));
        defaultIndexTimeout = settings.getAsTime("xpack.watcher.internal.ops.index.default_timeout", TimeValue.timeValueSeconds(60));
        defaultBulkTimeout = settings.getAsTime("xpack.watcher.internal.ops.bulk.default_timeout", TimeValue.timeValueSeconds(120));
    }

    /**
     * Creates a proxy to the given internal client (can be used for testing)
     */
    public static WatcherClientProxy of(Client client) {
        WatcherClientProxy proxy = new WatcherClientProxy(Settings.EMPTY);
        proxy.client = client instanceof InternalClient ? (InternalClient) client : new InternalClient.Insecure(client);
        return proxy;
    }

    public IndexResponse index(IndexRequest request, TimeValue timeout) {
        if (timeout == null) {
            timeout = defaultIndexTimeout;
        }
        return client.index(preProcess(request)).actionGet(timeout);
    }

    public UpdateResponse update(UpdateRequest request) {
        return client.update(preProcess(request)).actionGet(defaultIndexTimeout);
    }

    public BulkResponse bulk(BulkRequest request, TimeValue timeout) {
        if (timeout == null) {
            timeout = defaultBulkTimeout;
        }
        return client.bulk(preProcess(request)).actionGet(timeout);
    }

    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        client.index(preProcess(request), listener);
    }

    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        client.bulk(preProcess(request), listener);
    }

    public DeleteResponse delete(DeleteRequest request) {
        return client.delete(preProcess(request)).actionGet(defaultIndexTimeout);
    }

    public SearchResponse search(SearchRequest request, TimeValue timeout) {
        if (timeout == null) {
            timeout = defaultSearchTimeout;
        }
        return client.search(preProcess(request)).actionGet(timeout);
    }

    public SearchResponse searchScroll(String scrollId, TimeValue timeout) {
        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(timeout);
        return client.searchScroll(preProcess(request)).actionGet(defaultSearchTimeout);
    }

    public ClearScrollResponse clearScroll(String scrollId) {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        return client.clearScroll(preProcess(request)).actionGet(defaultSearchTimeout);
    }

    public RefreshResponse refresh(RefreshRequest request) {
        return client.admin().indices().refresh(preProcess(request)).actionGet(defaultSearchTimeout);
    }

    public PutIndexTemplateResponse putTemplate(PutIndexTemplateRequest request) {
        preProcess(request);
        return client.admin().indices().putTemplate(request).actionGet(defaultIndexTimeout);
    }
}
