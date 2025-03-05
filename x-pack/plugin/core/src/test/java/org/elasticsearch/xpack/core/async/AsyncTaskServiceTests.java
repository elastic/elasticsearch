/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

// TODO: test CRUD operations
public class AsyncTaskServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<AsyncSearchResponse> indexService;

    public String index = ".async-search";

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        indexService = new AsyncTaskIndexService<>(
            index,
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            "test_origin",
            AsyncSearchResponse::new,
            writableRegistry(),
            bigArrays
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    /**
     * This class exists because AsyncResultsIndexPlugin exists in a different x-pack module.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(AsyncTaskIndexService.getSystemIndexDescriptor());
        }

        @Override
        public String getFeatureName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return this.getClass().getCanonicalName();
        }
    }

    public void testAutoCreateIndex() throws Exception {
        // To begin with, the results index should be auto-created.
        AsyncExecutionId id = new AsyncExecutionId("0", new TaskId("N/A", 0));
        AsyncSearchResponse resp = new AsyncSearchResponse(id.getEncoded(), true, true, 0L, 0L);
        try {
            PlainActionFuture<DocWriteResponse> future = new PlainActionFuture<>();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        } finally {
            resp.decRef();
        }

        // Delete the index, so we can test subsequent auto-create behaviour
        assertAcked(client().admin().indices().prepareDelete(index));

        // Subsequent response deletes throw a (wrapped) index not found exception
        {
            PlainActionFuture<DeleteResponse> future = new PlainActionFuture<>();
            indexService.deleteResponse(id, future);
            expectThrows(Exception.class, future::get);
        }

        // So do updates
        {
            PlainActionFuture<UpdateResponse> future = new PlainActionFuture<>();
            indexService.updateResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            expectThrows(Exception.class, future::get);
            assertSettings();
        }

        // And so does updating the expiration time
        {
            PlainActionFuture<UpdateResponse> future = new PlainActionFuture<>();
            indexService.updateExpirationTime("0", 10L, future);
            expectThrows(Exception.class, future::get);
            assertSettings();
        }

        // But the index is still auto-created
        {
            PlainActionFuture<DocWriteResponse> future = new PlainActionFuture<>();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        }
    }

    private void assertSettings() {
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(index))
            .actionGet();
        Settings settings = getIndexResponse.getSettings().get(index);
        Settings expected = AsyncTaskIndexService.settings();
        assertThat(expected, is(settings.filter(expected::hasValue)));
    }

}
