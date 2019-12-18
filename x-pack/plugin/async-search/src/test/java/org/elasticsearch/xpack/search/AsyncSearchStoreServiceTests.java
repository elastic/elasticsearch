/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.assertEqualResponses;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.randomAsyncSearchResponse;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.randomSearchResponse;
import static org.elasticsearch.xpack.search.AsyncSearchStoreService.ASYNC_SEARCH_ALIAS;
import static org.elasticsearch.xpack.search.AsyncSearchStoreService.ASYNC_SEARCH_INDEX_PREFIX;
import static org.elasticsearch.xpack.search.GetAsyncSearchRequestTests.randomSearchId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class AsyncSearchStoreServiceTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;
    private ThreadPool threadPool;
    private VerifyingClient client;
    private AsyncSearchStoreService store;

    @Before
    public void setup() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        store = new AsyncSearchStoreService(client, namedWriteableRegistry);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testEncode() throws IOException {
        for (int i = 0; i < 10; i++) {
            AsyncSearchResponse response = randomAsyncSearchResponse(randomSearchId(), randomSearchResponse());
            String encoded = AsyncSearchStoreService.encodeResponse(response);
            AsyncSearchResponse same = AsyncSearchStoreService.decodeResponse(encoded, namedWriteableRegistry);
            assertEqualResponses(response, same);
        }
    }

    public void testIndexNeedsCreation() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder())
            .build();

        client.setVerifier((a, r, l) -> {
            assertThat(a, instanceOf(CreateIndexAction.class));
            assertThat(r, instanceOf(CreateIndexRequest.class));
            CreateIndexRequest request = (CreateIndexRequest) r;
            assertThat(request.aliases(), hasSize(1));
            request.aliases().forEach(alias -> {
                assertThat(alias.name(), equalTo(ASYNC_SEARCH_ALIAS));
                assertTrue(alias.writeIndex());
            });
            return new CreateIndexResponse(true, true, request.index());
        });

        CountDownLatch latch = new CountDownLatch(1);
        store.ensureAsyncSearchIndex(state, new LatchedActionListener<>(ActionListener.wrap(
            name -> assertThat(name, equalTo(ASYNC_SEARCH_INDEX_PREFIX + "000001")),
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testIndexProperlyExistsAlready() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder()
                .put(IndexMetaData.builder(ASYNC_SEARCH_INDEX_PREFIX + "000001")
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetaData.builder(ASYNC_SEARCH_ALIAS)
                        .writeIndex(true)
                        .build())))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        store.ensureAsyncSearchIndex(state, new LatchedActionListener<>(ActionListener.wrap(
            name -> assertThat(name, equalTo(ASYNC_SEARCH_INDEX_PREFIX + "000001")),
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testIndexHasNoWriteIndex() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder()
                .put(IndexMetaData.builder(ASYNC_SEARCH_INDEX_PREFIX + "000001")
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetaData.builder(ASYNC_SEARCH_ALIAS)
                        .build()))
                .put(IndexMetaData.builder(randomAlphaOfLength(5))
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetaData.builder(ASYNC_SEARCH_ALIAS)
                        .build())))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        store.ensureAsyncSearchIndex(state, new LatchedActionListener<>(ActionListener.wrap(
            name -> fail("should have called onFailure, not onResponse"),
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), containsString("async-search alias [" + ASYNC_SEARCH_ALIAS +
                    "] does not have a write index"));
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testIndexNotAlias() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder()
                .put(IndexMetaData.builder(ASYNC_SEARCH_ALIAS)
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        store.ensureAsyncSearchIndex(state, new LatchedActionListener<>(ActionListener.wrap(
            name -> fail("should have called onFailure, not onResponse"),
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), containsString("async-search alias [" + ASYNC_SEARCH_ALIAS +
                    "] already exists as concrete index"));
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testIndexCreatedConcurrently() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder())
            .build();

        client.setVerifier((a, r, l) -> {
            assertThat(a, instanceOf(CreateIndexAction.class));
            assertThat(r, instanceOf(CreateIndexRequest.class));
            CreateIndexRequest request = (CreateIndexRequest) r;
            assertThat(request.aliases(), hasSize(1));
            request.aliases().forEach(alias -> {
                assertThat(alias.name(), equalTo(ASYNC_SEARCH_ALIAS));
                assertTrue(alias.writeIndex());
            });
            throw new ResourceAlreadyExistsException("that index already exists");
        });

        CountDownLatch latch = new CountDownLatch(1);
        store.ensureAsyncSearchIndex(state, new LatchedActionListener<>(ActionListener.wrap(
            name -> assertThat(name, equalTo(ASYNC_SEARCH_INDEX_PREFIX + "000001")),
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testAliasDoesntExistButIndexDoes() throws InterruptedException {
        final String initialIndex = ASYNC_SEARCH_INDEX_PREFIX + "000001";
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder()
                .put(IndexMetaData.builder(initialIndex)
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        store.ensureAsyncSearchIndex(state, new LatchedActionListener<>(ActionListener.wrap(
            name -> {
                logger.error(name);
                fail("should have called onFailure, not onResponse");
            },
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), containsString("async-search index [" + initialIndex +
                    "] already exists but does not have alias [" + ASYNC_SEARCH_ALIAS + "]"));
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier = (a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            try {
                listener.onResponse((Response) verifier.apply(action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        public VerifyingClient setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            this.verifier = verifier;
            return this;
        }
    }
}
