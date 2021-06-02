/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

// TODO: test CRUD operations
public class AsyncTaskServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<AsyncSearchResponse> indexService;

    public String index = ".async-search";

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>(index, clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(), "test_origin", AsyncSearchResponse::new, writableRegistry());
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

    public void testEnsuredAuthenticatedUserIsSame() throws IOException {
        Authentication original =
            new Authentication(new User("test", "role"), new Authentication.RealmRef("realm", "file", "node"), null);
        Authentication current = randomBoolean() ? original :
            new Authentication(new User("test", "role"), new Authentication.RealmRef("realm", "file", "node"), null);
        assertTrue(original.canAccessResourcesOf(current));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        original.writeToContext(threadContext);
        assertTrue(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), current));

        // original is not set
        assertTrue(indexService.ensureAuthenticatedUserIsSame(Collections.emptyMap(), current));
        // current is not set
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), null));

        // original user being run as
        User user = new User(new User("test", "role"), new User("authenticated", "runas"));
        current = new Authentication(user, new Authentication.RealmRef("realm", "file", "node"),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        assertTrue(original.canAccessResourcesOf(current));
        assertTrue(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), current));

        // both user are run as
        current = new Authentication(user, new Authentication.RealmRef("realm", "file", "node"),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        Authentication runAs = current;
        assertTrue(runAs.canAccessResourcesOf(current));
        threadContext = new ThreadContext(Settings.EMPTY);
        original.writeToContext(threadContext);
        assertTrue(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), current));

        // different authenticated by type
        Authentication differentRealmType =
            new Authentication(new User("test", "role"), new Authentication.RealmRef("realm", randomAlphaOfLength(5), "node"), null);
        threadContext = new ThreadContext(Settings.EMPTY);
        original.writeToContext(threadContext);
        assertFalse(original.canAccessResourcesOf(differentRealmType));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), differentRealmType));

        // wrong user
        Authentication differentUser =
            new Authentication(new User("test2", "role"), new Authentication.RealmRef("realm", "realm", "node"), null);
        assertFalse(original.canAccessResourcesOf(differentUser));

        // run as different user
        Authentication diffRunAs = new Authentication(new User(new User("test2", "role"), new User("authenticated", "runas")),
            new Authentication.RealmRef("realm", "file", "node1"), new Authentication.RealmRef("realm", "file", "node1"));
        assertFalse(original.canAccessResourcesOf(diffRunAs));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), diffRunAs));

        // run as different looked up by type
        Authentication runAsDiffType = new Authentication(user, new Authentication.RealmRef("realm", "file", "node"),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 12), "node"));
        assertFalse(original.canAccessResourcesOf(runAsDiffType));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), runAsDiffType));
    }

    public void testAutoCreateIndex() throws Exception {
        // To begin with, the results index should be auto-created.
        AsyncExecutionId id = new AsyncExecutionId("0", new TaskId("N/A", 0));
        AsyncSearchResponse resp = new AsyncSearchResponse(id.getEncoded(), true, true, 0L, 0L);
        {
            PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        }

        // Delete the index, so we can test subsequent auto-create behaviour
        AcknowledgedResponse ack = client().admin().indices().prepareDelete(index).get();
        assertTrue(ack.isAcknowledged());

        // Subsequent response deletes throw a (wrapped) index not found exception
        {
            PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
            indexService.deleteResponse(id, future);
            expectThrows(Exception.class, future::get);
        }

        // And so does updating the expiration time
        {
            PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
            indexService.updateExpirationTime("0", 10L, future);
            expectThrows(Exception.class, future::get);
            assertSettings();
        }

        // But the index is still auto-created
        {
            PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        }
    }

    public void testUpdateDeletedResponse() throws Exception {
        AsyncExecutionId id = new AsyncExecutionId("0", new TaskId("N/A", 0));
        {
            AsyncSearchResponse resp = new AsyncSearchResponse(id.getEncoded(), true, true, 0L, 0L);
            PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
        }
        CountDownLatch latch = new CountDownLatch(1);
        Thread updateThread = new Thread(() -> {
            latch.countDown();
            for (int i = 0; i < 100; i++) {
                SearchResponse searchResponse = randomBoolean() ? null
                    : new SearchResponse(InternalSearchResponse.empty(), randomAlphaOfLength(10), 1, 1, 0, randomIntBetween(0, 10000),
                    ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
                AsyncSearchResponse newResponse = new AsyncSearchResponse(id.getEncoded(),
                    searchResponse, null, true, true, randomLong(), randomLong());
                PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
                indexService.updateResponse(id, Map.of(), newResponse, future);
                try {
                    future.get();
                } catch (Exception e) {
                    PlainActionFuture<AsyncSearchResponse> getFuture = PlainActionFuture.newFuture();
                    indexService.getResponse(id, randomBoolean(), getFuture);
                    expectThrows(ResourceNotFoundException.class, getFuture::actionGet);
                    return;
                }
            }
        });
        updateThread.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        {
            PlainActionFuture<DeleteResponse> deleteFuture = PlainActionFuture.newFuture();
            indexService.deleteResponse(id, deleteFuture);
            deleteFuture.actionGet();
        }
        updateThread.join(30_000, 0);
    }

    public void testUpdateResponseAndExpirationTimeConcurrently() throws Exception {
        assertThat(null, equalTo(null));
        AsyncExecutionId executionId = new AsyncExecutionId("0", new TaskId("N/A", 0));
        long lastExpirationTime = randomLong();
        AsyncSearchResponse initialResponse = new AsyncSearchResponse(executionId.getEncoded(), true, true, 0L, lastExpirationTime);
        {
            PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
            indexService.createResponse(executionId.getDocId(), Collections.emptyMap(), initialResponse, future);
            future.actionGet();
        }
        CountDownLatch latch = new CountDownLatch(1);
        Thread updateResponseThread = new Thread(() -> {
            latch.countDown();
            SearchResponse searchResponse = null;
            int iterations = between(1, 5);
            for (int i = 0; i < iterations; i++) {
                if (randomBoolean()) {
                    PlainActionFuture<AsyncSearchResponse> getFuture = PlainActionFuture.newFuture();
                    indexService.getResponse(executionId, randomBoolean(), getFuture);
                    final SearchResponse actualResponse = getFuture.actionGet().getSearchResponse();
                    if (searchResponse == null) {
                        assertNull(actualResponse);
                    } else {
                        assertThat(Strings.toString(actualResponse), equalTo(Strings.toString(searchResponse)));
                    }
                }
                searchResponse = randomBoolean() ? null
                    : new SearchResponse(InternalSearchResponse.empty(), randomAlphaOfLength(10), 1, 1, 0, randomIntBetween(0, 10000),
                    ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);

                AsyncSearchResponse newResponse = new AsyncSearchResponse(executionId.getEncoded(),
                    searchResponse, null, true, true, randomLong(), randomLong());
                PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
                indexService.updateResponse(executionId, Map.of(), newResponse, future);
                future.actionGet();
            }
        });
        updateResponseThread.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        int iterations = between(1, 5);
        for (int i = 0; i < iterations; i++) {
            if (randomBoolean()) {
                PlainActionFuture<AsyncSearchResponse> getFuture = PlainActionFuture.newFuture();
                indexService.getResponse(executionId, randomBoolean(), getFuture);
                assertThat(getFuture.actionGet().getExpirationTime(), equalTo(lastExpirationTime));
            }
            lastExpirationTime = randomNonNegativeLong();
            PlainActionFuture<UpdateResponse> updateFuture = PlainActionFuture.newFuture();
            indexService.updateExpirationTime(executionId.getDocId(), lastExpirationTime, updateFuture);
            updateFuture.actionGet();
        }
        updateResponseThread.join(60_000, 0);
    }

    private void assertSettings() {
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(
            new GetIndexRequest().indices(index)).actionGet();
        Settings settings = getIndexResponse.getSettings().get(index);
        Settings expected = AsyncTaskIndexService.settings();
        assertEquals(expected, settings.filter(expected::hasValue));
    }
}
