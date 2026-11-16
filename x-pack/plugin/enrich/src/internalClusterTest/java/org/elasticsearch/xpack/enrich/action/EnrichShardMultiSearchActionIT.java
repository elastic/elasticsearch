/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.LocalStateEnrich;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EnrichShardMultiSearchActionIT extends ESIntegTestCase {

    private static final int NUM_DOCS = 10;
    private static final int SOURCE_LENGTH = 10 * 1024;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateEnrich.class, MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // TODO Change this to run with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            // pin a real request breaker, overriding the test cluster's random noop one, which always reports 0
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    /**
     * When the lookup comes from another node, the hit sources must stay charged until the response is sent, because they are
     * still on the heap until it has been serialized.
     */
    public void testChargeIsHeldUntilResponseIsSent() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        // no copy of the enrich index here, so the lookup is sent to the data node
        String coordinatingNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        String indexName = EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1";
        createIndex(indexName, indexSettings(1, 0).build());
        for (int i = 0; i < NUM_DOCS; i++) {
            XContentBuilder source = XContentBuilder.builder(XContentType.SMILE.xContent());
            source.startObject();
            source.field("key", randomAlphaOfLength(SOURCE_LENGTH));
            source.endObject();
            client().index(new IndexRequest(indexName).source(source)).actionGet();
        }
        refresh(indexName);

        CircuitBreaker breaker = internalCluster().getInstance(CircuitBreakerService.class, dataNode).getBreaker(CircuitBreaker.REQUEST);
        AtomicLong usedWhileSending = new AtomicLong(-1);
        MockTransportService.getInstance(dataNode)
            .addRequestHandlingBehavior(
                EnrichShardMultiSearchAction.INSTANCE.name() + "[s]",
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        usedWhileSending.set(breaker.getUsed());
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

        int numSearches = 4;
        MultiSearchRequest request = new MultiSearchRequest();
        for (int i = 0; i < numSearches; i++) {
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source().from(0);
            searchRequest.source().size(NUM_DOCS);
            searchRequest.source().query(new MatchAllQueryBuilder());
            searchRequest.source().fetchSource(true);
            request.add(searchRequest);
        }

        assertResponse(
            client(coordinatingNode).execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)),
            response -> {
                for (MultiSearchResponse.Item item : response.getResponses()) {
                    assertThat(item.isFailure(), is(false));
                    assertThat(item.getResponse().getHits().getHits().length, equalTo(NUM_DOCS));
                }
            }
        );
        // every hit's source is at least SOURCE_LENGTH bytes
        assertThat(usedWhileSending.get(), greaterThanOrEqualTo((long) numSearches * NUM_DOCS * SOURCE_LENGTH));
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(0L)));
    }
}
