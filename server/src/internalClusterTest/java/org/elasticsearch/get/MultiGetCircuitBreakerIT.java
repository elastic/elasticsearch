/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.get;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.ShardMultiGetFromTranslogUtil;
import org.elasticsearch.action.get.TransportShardMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.elasticsearch.action.get.ShardMultiGetFromTranslogUtil.getFailures;
import static org.elasticsearch.action.get.ShardMultiGetFromTranslogUtil.getResponses;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Document sources are charged to the request breaker of the node holding the shard until its response has been serialized. A document
 * the breaker has no room for fails alone with a 429, and the other documents are still returned.
 */
public class MultiGetCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX = "test";
    private static final int LARGE_SOURCE_CHARS = 3 * 1024 * 1024;
    private static final String REQUEST_LIMIT = "2mb";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // the test cluster randomly makes the request breaker a no-op
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class),
            UnavailableDocumentEnginePlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSourceStaysChargedUntilShardResponseIsSent() throws Exception {
        assertAcked(prepareCreate(INDEX).setMapping("payload", "type=keyword,index=false,doc_values=false"));
        indexRandom(true, prepareIndex(INDEX).setId("large").setSource("payload", randomAlphaOfLength(LARGE_SOURCE_CHARS)));

        AtomicLong usedWhenSent = new AtomicLong(-1);
        // records the request breaker when the shard response is sent
        for (String node : internalCluster().getNodeNames()) {
            CircuitBreaker breaker = internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST);
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(
                    TransportShardMultiGetAction.TYPE.name() + "[s]",
                    (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            usedWhenSent.set(breaker.getUsed());
                            channel.sendResponse(response);
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }

                        @Override
                        public TransportVersion getVersion() {
                            return channel.getVersion();
                        }
                    }, task)
                );
        }

        MultiGetResponse response = client().prepareMultiGet()
            .add(INDEX, "large")
            .setRealtime(randomBoolean())
            .setRefresh(randomBoolean())
            .get();
        assertTrue(response.getResponses()[0].getResponse().isExists());
        assertThat(usedWhenSent.get(), greaterThanOrEqualTo((long) LARGE_SOURCE_CHARS));
        assertRequestBreakersReleased();
    }

    public void testDocumentWithoutRoomInBreakerFailsAlone() throws Exception {
        assertAcked(prepareCreate(INDEX).setMapping("payload", "type=keyword,index=false,doc_values=false"));
        indexRandom(
            true,
            prepareIndex(INDEX).setId("large").setSource("payload", randomAlphaOfLength(LARGE_SOURCE_CHARS)),
            prepareIndex(INDEX).setId("small").setSource("payload", "small")
        );

        MultiGetResponse response = multiGetLargeAndSmall();
        assertFalse(response.getResponses()[0].isFailed());
        assertTrue(response.getResponses()[0].getResponse().isExists());
        assertRequestBreakersReleased();

        updateClusterSettings(Settings.builder().put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), REQUEST_LIMIT));
        try {
            response = multiGetLargeAndSmall();
            assertTripped(response.getResponses()[0].getFailure());
            MultiGetItemResponse small = response.getResponses()[1];
            assertFalse(small.isFailed());
            assertTrue(small.getResponse().isExists());
        } finally {
            updateClusterSettings(Settings.builder().putNull(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()));
        }
        assertRequestBreakersReleased();
    }

    /**
     * In Stateless, a search shard gets the documents it can't see yet from the indexing shard, which loads them from the translog. This
     * calls the from-translog action directly, as a search shard would.
     */
    public void testDocumentFromTranslogWithoutRoomInBreakerFailsAlone() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(indexSettings(1, 0).put("index.refresh_interval", -1))
                .setMapping("payload", "type=keyword,index=false,doc_values=false")
        );
        ensureGreen(INDEX);
        // A realtime get makes the engine store translog locations, so that the documents indexed next can be read from the translog
        prepareIndex(INDEX).setId("0").setSource("payload", "first").get();
        client().prepareGet(INDEX, "0").get();
        prepareIndex(INDEX).setId("large").setSource("payload", randomAlphaOfLength(LARGE_SOURCE_CHARS)).get();
        prepareIndex(INDEX).setId("small").setSource("payload", "small").get();
        ShardRouting shard = clusterService().state().routingTable().index(INDEX).shard(0).primaryShard();

        updateClusterSettings(Settings.builder().put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), REQUEST_LIMIT));
        try {
            var response = getFromTranslog(shard, List.of("large", "small")).multiGetShardResponse();
            assertTripped(getFailures(response).get(0));
            assertNotNull(getResponses(response).get(1));
            assertTrue(getResponses(response).get(1).isExists());
        } finally {
            updateClusterSettings(Settings.builder().putNull(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()));
        }
        assertRequestBreakersReleased();
    }

    /**
     * The charge is released when the shard request fails as a whole after a document has been charged, as when a later document finds
     * the shard unavailable.
     */
    public void testSourceIsReleasedWhenShardFailsAfterCharging() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(indexSettings(1, 0).put("index.refresh_interval", -1))
                .setMapping("payload", "type=keyword,index=false,doc_values=false")
        );
        ensureGreen(INDEX);
        // A realtime get makes the engine store translog locations, so that the document indexed next can be read from the translog
        prepareIndex(INDEX).setId("0").setSource("payload", "first").get();
        client().prepareGet(INDEX, "0").get();
        prepareIndex(INDEX).setId("large").setSource("payload", randomAlphaOfLength(LARGE_SOURCE_CHARS)).get();
        ShardRouting shard = clusterService().state().routingTable().index(INDEX).shard(0).primaryShard();

        MultiGetResponse response = client().prepareMultiGet()
            .add(INDEX, "large")
            .add(INDEX, UnavailableDocumentEnginePlugin.ID)
            .setRealtime(true)
            .get();
        assertTrue(response.getResponses()[0].isFailed());
        assertTrue(response.getResponses()[1].isFailed());

        var fromTranslog = sendGetFromTranslog(shard, List.of("large", UnavailableDocumentEnginePlugin.ID));
        Exception e = expectThrows(Exception.class, fromTranslog::actionGet);
        assertTrue(TransportActions.isShardNotAvailableException(e));
        assertRequestBreakersReleased();
    }

    private MultiGetResponse multiGetLargeAndSmall() {
        return client().prepareMultiGet()
            .add(INDEX, "large")
            .add(INDEX, "small")
            .setRealtime(randomBoolean())
            .setRefresh(randomBoolean())
            .get();
    }

    private static void assertTripped(MultiGetResponse.Failure failure) {
        assertNotNull("the large document should have failed", failure);
        assertThat(ExceptionsHelper.status(failure.getFailure()), equalTo(RestStatus.TOO_MANY_REQUESTS));
        Throwable trip = ExceptionsHelper.unwrap(failure.getFailure(), CircuitBreakingException.class);
        assertThat(trip, notNullValue());
        assertThat(trip.getMessage(), containsString("[get[source]]"));
    }

    private static void assertRequestBreakersReleased() throws Exception {
        for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
            CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(0L)));
        }
    }

    private TransportShardMultiGetFomTranslogAction.Response getFromTranslog(ShardRouting shard, List<String> ids) {
        return safeGet(sendGetFromTranslog(shard, ids));
    }

    private PlainActionFuture<TransportShardMultiGetFomTranslogAction.Response> sendGetFromTranslog(ShardRouting shard, List<String> ids) {
        var multiGetRequest = client().prepareMultiGet().addIds(INDEX, ids).request();
        var request = new TransportShardMultiGetFomTranslogAction.Request(
            ShardMultiGetFromTranslogUtil.newMultiGetShardRequest(multiGetRequest, shard.shardId()),
            shard.shardId()
        );
        var transportService = internalCluster().getInstance(TransportService.class);
        PlainActionFuture<TransportShardMultiGetFomTranslogAction.Response> response = new PlainActionFuture<>();
        transportService.sendRequest(
            clusterService().state().nodes().get(shard.currentNodeId()),
            TransportShardMultiGetFomTranslogAction.NAME,
            request,
            new ActionListenerResponseHandler<>(
                response,
                TransportShardMultiGetFomTranslogAction.Response::new,
                transportService.getThreadPool().executor(ThreadPool.Names.GET)
            )
        );
        return response;
    }

    /**
     * Fails the gets of one document as if the engine had been closed, which fails the whole shard request rather than just the document.
     */
    public static class UnavailableDocumentEnginePlugin extends Plugin implements EnginePlugin {

        static final String ID = "unavailable";

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                public GetResult get(
                    Get get,
                    MappingLookup mappingLookup,
                    DocumentParser documentParser,
                    SplitShardCountSummary splitShardCountSummary,
                    Function<Searcher, Searcher> searcherWrapper
                ) {
                    failIfUnavailable(get);
                    return super.get(get, mappingLookup, documentParser, splitShardCountSummary, searcherWrapper);
                }

                @Override
                public GetResult getFromTranslog(
                    Get get,
                    MappingLookup mappingLookup,
                    DocumentParser documentParser,
                    Function<Searcher, Searcher> searcherWrapper
                ) {
                    failIfUnavailable(get);
                    return super.getFromTranslog(get, mappingLookup, documentParser, searcherWrapper);
                }
            });
        }

        private static void failIfUnavailable(Engine.Get get) {
            if (get.id().equals(ID)) {
                throw new AlreadyClosedException("engine is closed");
            }
        }
    }
}
