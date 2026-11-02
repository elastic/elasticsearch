/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.LocalStateEnrich;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class EnrichShardMultiSearchActionTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class);
    }

    public void testExecute() throws Exception {
        XContentBuilder source = XContentBuilder.builder(XContentType.SMILE.xContent());
        source.startObject();
        source.field("key1", "value1");
        source.field("key2", "value2");
        source.endObject();

        String indexName = EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1";
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source(source);
        client().index(indexRequest).actionGet();
        client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

        int numSearches = randomIntBetween(2, 32);
        MultiSearchRequest request = new MultiSearchRequest();
        for (int i = 0; i < numSearches; i++) {
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source().from(0);
            searchRequest.source().size(1);
            searchRequest.source().query(new MatchAllQueryBuilder());
            searchRequest.source().fetchSource("key1", null);
            request.add(searchRequest);
        }

        assertResponse(
            client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)),
            response -> {
                assertThat(response.getResponses().length, equalTo(numSearches));
                for (int i = 0; i < numSearches; i++) {
                    assertThat(response.getResponses()[i].isFailure(), is(false));
                    assertThat(response.getResponses()[i].getResponse().getHits().getTotalHits().value(), equalTo(1L));
                    Map<String, Object> sourceAsMap = response.getResponses()[i].getResponse().getHits().getHits()[0].getSourceAsMap();
                    assertThat(sourceAsMap.size(), equalTo(1));
                    assertThat(sourceAsMap.get("key1"), equalTo("value1"));
                }
            }
        );

    }

    public void testNonEnrichIndex() throws Exception {
        createIndex("index");
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest("index"));
        Exception e = expectThrows(
            ActionRequestValidationException.class,
            client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request))
        );
        assertThat(e.getMessage(), equalTo("Validation Failed: 1: index [index] is not an enrich index;"));
    }

    public void testMultipleShards() throws Exception {
        String indexName = EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 2).build());
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest(indexName));
        Exception e = expectThrows(
            IllegalStateException.class,
            client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request))
        );
        assertThat(e.getMessage(), equalTo("index [.enrich-1] should have 1 shard, but has 2 shards"));
    }

    /** A lookup whose hit sources do not fit in the request breaker fails instead of holding them all on the heap. */
    public void testTripsRequestBreaker() throws Exception {
        String indexName = indexLargeDocuments(10);
        MultiSearchRequest request = matchAllSearches(indexName, 32, 10);
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        long usedBefore = breaker.getUsed();

        // the response would hold 32 searches x 10 hits x 10kb of source
        assertLookupSucceeds(request);
        updateClusterSettings(Settings.builder().put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb").build());
        try {
            CircuitBreakingException e = expectThrows(
                CircuitBreakingException.class,
                client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request))
            );
            assertThat(e.getMessage(), containsString(EnrichShardMultiSearchAction.BREAKER_LABEL));
        } finally {
            updateClusterSettings(Settings.builder().putNull(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()).build());
        }
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
    }

    /** Sources that stay below the charge threshold are still charged once the response is complete. */
    public void testChargesResponsesBelowTheThreshold() throws Exception {
        String indexName = indexLargeDocuments(10);
        // 4 searches x 10 hits x 10kb of source, which is less than the threshold but more than the breaker limit below
        MultiSearchRequest request = matchAllSearches(indexName, 4, 10);
        assertThat(400 * 1024L, lessThan(EnrichShardMultiSearchAction.BREAKER_CHARGE_THRESHOLD_BYTES));
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        long usedBefore = breaker.getUsed();

        assertLookupSucceeds(request);
        updateClusterSettings(Settings.builder().put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100kb").build());
        try {
            CircuitBreakingException e = expectThrows(
                CircuitBreakingException.class,
                client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request))
            );
            assertThat(e.getMessage(), containsString(EnrichShardMultiSearchAction.BREAKER_LABEL));
        } finally {
            updateClusterSettings(Settings.builder().putNull(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()).build());
        }
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
    }

    /** The release goes to the category of the charge, so that the per-category memory gauge returns to zero. */
    public void testReservationReleasesUnderItsCategory() {
        RecordingMeterRegistry meter = new RecordingMeterRegistry();
        // exposes a recording meter registry, so that the held-usage gauge can be read per category
        CircuitBreakerMetrics metrics = new CircuitBreakerMetrics(new TelemetryProvider.NoopTelemetryProvider() {
            @Override
            public MeterRegistry getMeterRegistry() {
                return meter;
            }
        });
        // without the real-memory parent check, so that the test does not depend on the heap usage of the test JVM
        Settings settings = Settings.builder().put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false).build();
        HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
            metrics,
            settings,
            Collections.emptyList(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        var reservation = new EnrichShardMultiSearchAction.SourceBytesReservation(service.getBreaker(CircuitBreaker.REQUEST));

        reservation.add(EnrichShardMultiSearchAction.BREAKER_CHARGE_THRESHOLD_BYTES);
        reservation.decRef();

        Map<Object, Long> heldByCategory = meter.getRecorder()
            .getMeasurements(InstrumentType.LONG_UP_DOWN_COUNTER, CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)
            .stream()
            .filter(m -> CircuitBreaker.REQUEST.equals(m.attributes().get(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE)))
            .collect(
                Collectors.groupingBy(
                    m -> m.attributes().get(ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE),
                    Collectors.summingLong(Measurement::getLong)
                )
            );
        assertThat(heldByCategory.get(ChildMemoryCircuitBreaker.CATEGORY_MSEARCH), equalTo(0L));
        assertThat(heldByCategory.getOrDefault(ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED, 0L), equalTo(0L));
    }

    /** Every step charged on the way is released, not only the last one. */
    public void testReservationReleasesEveryCharge() {
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        long usedBefore = breaker.getUsed();
        long step = EnrichShardMultiSearchAction.BREAKER_CHARGE_THRESHOLD_BYTES * 3 / 2;
        var reservation = new EnrichShardMultiSearchAction.SourceBytesReservation(breaker);

        reservation.add(step);
        assertThat(breaker.getUsed(), equalTo(usedBefore + step));
        reservation.add(step);
        assertThat(breaker.getUsed(), equalTo(usedBefore + 2 * step));

        reservation.decRef();
        assertThat(breaker.getUsed(), equalTo(usedBefore));
    }

    /** A local caller gets the response object itself, so its hits stay charged until the caller has consumed it. */
    public void testChargeIsHeldWhileLocalResponseIsHeld() throws Exception {
        String indexName = indexLargeDocuments(10);
        MultiSearchRequest request = matchAllSearches(indexName, 4, 10);
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        long usedBefore = breaker.getUsed();

        assertResponse(
            client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)),
            // 4 searches x 10 hits, each with at least 10kb of source
            response -> assertThat(breaker.getUsed(), greaterThanOrEqualTo(usedBefore + 4 * 10 * 10 * 1024L))
        );
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
    }

    /**
     * The hits stay charged while the coordinator hands the response to the enrich processors. The batch targets two enrich indices,
     * because the coordinator frees the shard response of the index that answered first before it hands out the items.
     */
    public void testChargeIsHeldWhileCoordinatorHandsOutResponse() throws Exception {
        MultiSearchRequest request = new MultiSearchRequest();
        for (String indexName : List.of(EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1", EnrichPolicy.ENRICH_INDEX_NAME_BASE + "2")) {
            indexLargeDocuments(indexName, 10);
            request.add(matchAllSearches(indexName, 1, 10).requests().get(0));
        }
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        long usedBefore = breaker.getUsed();

        // a lookup failure reaches the consumer as a failed item, so it is recorded next to the breaker reading rather than lost
        record Observed(long used, List<Exception> failures) {}
        PlainActionFuture<Observed> observed = new PlainActionFuture<>();
        EnrichCoordinatorProxyAction.Coordinator.lookupFunction(client())
            .accept(
                request,
                (response, e) -> ActionListener.completeWith(
                    observed,
                    () -> new Observed(
                        breaker.getUsed(),
                        Arrays.stream(response.getResponses())
                            .filter(MultiSearchResponse.Item::isFailure)
                            .map(MultiSearchResponse.Item::getFailure)
                            .toList()
                    )
                )
            );

        Observed result = safeGet(observed);
        assertThat(result.failures(), empty());
        // 2 indices x 10 hits, each with at least 10kb of source
        assertThat(result.used(), greaterThanOrEqualTo(usedBefore + 2 * 10 * 10 * 1024L));
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
    }

    /** The bytes charged while the response is built are released once the response has been handled. */
    public void testReleasesRequestBreakerAfterResponse() throws Exception {
        String indexName = indexLargeDocuments(10);
        MultiSearchRequest request = matchAllSearches(indexName, 4, 10);
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        long usedBefore = breaker.getUsed();

        assertResponse(
            client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)),
            response -> {
                for (MultiSearchResponse.Item item : response.getResponses()) {
                    assertThat(item.isFailure(), is(false));
                    assertThat(item.getResponse().getHits().getHits().length, equalTo(10));
                }
            }
        );
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(usedBefore)));
    }

    private void assertLookupSucceeds(MultiSearchRequest request) throws Exception {
        assertResponse(
            client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)),
            response -> {
                for (MultiSearchResponse.Item item : response.getResponses()) {
                    assertThat(item.isFailure(), is(false));
                }
            }
        );
    }

    private String indexLargeDocuments(int numDocs) throws Exception {
        return indexLargeDocuments(EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1", numDocs);
    }

    private String indexLargeDocuments(String indexName, int numDocs) throws Exception {
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = XContentBuilder.builder(XContentType.SMILE.xContent());
            source.startObject();
            source.field("key", randomAlphaOfLength(10 * 1024));
            source.endObject();
            client().index(new IndexRequest(indexName).source(source)).actionGet();
        }
        client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
        return indexName;
    }

    /** Searches built the way the enrich processor builds them: the full source of up to {@code size} matches. */
    private static MultiSearchRequest matchAllSearches(String indexName, int numSearches, int size) {
        MultiSearchRequest request = new MultiSearchRequest();
        for (int i = 0; i < numSearches; i++) {
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source().from(0);
            searchRequest.source().size(size);
            searchRequest.source().query(new MatchAllQueryBuilder());
            searchRequest.source().fetchSource(true);
            request.add(searchRequest);
        }
        return request;
    }

    public void testMultipleIndices() throws Exception {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest("index1"));
        request.add(new SearchRequest("index2"));
        expectThrows(AssertionError.class, () -> new EnrichShardMultiSearchAction.Request(request));
    }

}
