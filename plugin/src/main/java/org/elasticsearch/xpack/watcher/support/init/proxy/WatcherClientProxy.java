/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support.init.proxy;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.xpack.common.init.proxy.ClientProxy;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.io.IOException;
import java.util.Collections;

/**
 * A lazily initialized proxy to an elasticsearch {@link Client}. Inject this proxy whenever a client
 * needs to injected to be avoid circular dependencies issues.
 */
public class WatcherClientProxy extends ClientProxy {

    private final TimeValue defaultSearchTimeout;
    private final TimeValue defaultIndexTimeout;
    private final TimeValue defaultBulkTimeout;

    public WatcherClientProxy(Settings settings, InternalClient client) {
        super(client);
        defaultSearchTimeout = settings.getAsTime("xpack.watcher.internal.ops.search.default_timeout", TimeValue.timeValueSeconds(30));
        defaultIndexTimeout = settings.getAsTime("xpack.watcher.internal.ops.index.default_timeout", TimeValue.timeValueSeconds(60));
        defaultBulkTimeout = settings.getAsTime("xpack.watcher.internal.ops.bulk.default_timeout", TimeValue.timeValueSeconds(120));
    }

    /**
     * Creates a proxy to the given internal client (can be used for testing)
     */
    public static WatcherClientProxy of(Client client) {
        return new WatcherClientProxy(Settings.EMPTY, client instanceof InternalClient ? (InternalClient) client :
                new InternalClient(client.settings(), client.threadPool(), client));
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

    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        client.update(preProcess(request), listener);
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

    public void putTemplate(PutIndexTemplateRequest request, ActionListener<PutIndexTemplateResponse> listener) {
        preProcess(request);
        client.admin().indices().putTemplate(request, listener);
    }

    public GetResponse getWatch(String id) {
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        getWatch(id, future);
        return future.actionGet();
    }

    public void getWatch(String id, ActionListener<GetResponse> listener) {
        GetRequest getRequest = new GetRequest(Watch.INDEX, Watch.DOC_TYPE, id).preference(Preference.LOCAL.type()).realtime(true);
        getRequest.realtime(true);
        client.get(preProcess(getRequest), listener);
    }

    public void deleteWatch(String id, ActionListener<DeleteResponse> listener) {
        DeleteRequest request = new DeleteRequest(Watch.INDEX, Watch.DOC_TYPE, id);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(preProcess(request), listener);
    }

    /**
     * Updates and persists the status of the given watch
     *
     * If the watch is missing (because it might have been deleted by the user during an execution), then this method
     * does nothing and just returns without throwing an exception
     */
    public void updateWatchStatus(Watch watch) throws IOException {
        // at the moment we store the status together with the watch,
        // so we just need to update the watch itself
        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(Watch.INCLUDE_STATUS_KEY, "true"));
        XContentBuilder source = JsonXContent.contentBuilder().
                startObject()
                .field(Watch.Field.STATUS.getPreferredName(), watch.status(), params)
                .endObject();

        UpdateRequest updateRequest = new UpdateRequest(Watch.INDEX, Watch.DOC_TYPE, watch.id());
        updateRequest.doc(source);
        updateRequest.version(watch.version());
        try {
            this.update(updateRequest);
        } catch (DocumentMissingException e) {
            // do not rethrow this exception, otherwise the watch history will contain an exception
            // even though the execution might have been fine
        }
    }

}
