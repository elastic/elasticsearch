/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase.SuiteScopeTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@SuiteScopeTestCase
public class AsyncSearchActionIT extends AsyncSearchIntegTestCase {
    private static String indexName;
    private static int numShards;

    private static int numKeywords;
    private static Map<String, AtomicInteger> keywordFreqs;
    private static float maxMetric = Float.NEGATIVE_INFINITY;
    private static float minMetric = Float.POSITIVE_INFINITY;

    @Override
    public void setupSuiteScopeCluster() throws InterruptedException {
        indexName = "test-async";
        numShards = randomIntBetween(1, 20);
        int numDocs = randomIntBetween(100, 1000);
        createIndex(indexName, Settings.builder().put("index.number_of_shards", numShards).build());
        numKeywords = randomIntBetween(50, 100);
        keywordFreqs = new HashMap<>();
        Set<String> keywordSet = new HashSet<>();
        for (int i = 0; i < numKeywords; i++) {
            keywordSet.add(randomAlphaOfLengthBetween(10, 20));
        }
        numKeywords = keywordSet.size();
        String[] keywords = keywordSet.toArray(String[]::new);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            float metric = randomFloat();
            maxMetric = Math.max(metric, maxMetric);
            minMetric = Math.min(metric, minMetric);
            String keyword = keywords[randomIntBetween(0, numKeywords - 1)];
            keywordFreqs.compute(keyword, (k, v) -> {
                if (v == null) {
                    return new AtomicInteger(1);
                }
                v.incrementAndGet();
                return v;
            });
            reqs.add(prepareIndex(indexName).setSource("terms", keyword, "metric", metric));
        }
        indexRandom(true, true, reqs);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
    }

    public void testMaxMinAggregation() throws Exception {
        int step = numShards > 2 ? randomIntBetween(2, numShards) : 2;
        int numFailures = randomBoolean() ? randomIntBetween(0, numShards) : 0;
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(AggregationBuilders.min("min").field("metric"))
            .aggregation(AggregationBuilders.max("max").field("metric"));
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, source, numFailures, step)) {
            AsyncSearchResponse response = it.next();
            try {
                while (it.hasNext()) {
                    response.decRef();
                    response = it.next();
                    assertNotNull(response.getSearchResponse());
                    if (response.getSearchResponse().getSuccessfulShards() > 0) {
                        assertNotNull(response.getSearchResponse().getAggregations());
                        assertNotNull(response.getSearchResponse().getAggregations().get("max"));
                        assertNotNull(response.getSearchResponse().getAggregations().get("min"));
                        Max max = response.getSearchResponse().getAggregations().get("max");
                        Min min = response.getSearchResponse().getAggregations().get("min");
                        assertThat((float) min.value(), greaterThanOrEqualTo(minMetric));
                        assertThat((float) max.value(), lessThanOrEqualTo(maxMetric));
                    }
                }
                if (numFailures == numShards) {
                    assertNotNull(response.getFailure());
                } else {
                    assertNotNull(response.getSearchResponse());
                    assertNotNull(response.getSearchResponse().getAggregations());
                    assertNotNull(response.getSearchResponse().getAggregations().get("max"));
                    assertNotNull(response.getSearchResponse().getAggregations().get("min"));
                    Max max = response.getSearchResponse().getAggregations().get("max");
                    Min min = response.getSearchResponse().getAggregations().get("min");
                    if (numFailures == 0) {
                        assertThat((float) min.value(), equalTo(minMetric));
                        assertThat((float) max.value(), equalTo(maxMetric));
                    } else {
                        assertThat((float) min.value(), greaterThanOrEqualTo(minMetric));
                        assertThat((float) max.value(), lessThanOrEqualTo(maxMetric));
                    }
                }
                deleteAsyncSearch(response.getId());
                ensureTaskRemoval(response.getId());
            } finally {
                response.decRef();
            }
        }
    }

    public void testTermsAggregation() throws Exception {
        int step = numShards > 2 ? randomIntBetween(2, numShards) : 2;
        int numFailures = randomBoolean() ? randomIntBetween(0, numShards) : 0;
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            AggregationBuilders.terms("terms").field("terms.keyword").size(numKeywords)
        );
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, source, numFailures, step)) {
            AsyncSearchResponse response = it.next();
            try {
                while (it.hasNext()) {
                    response.decRef();
                    response = it.next();
                    assertNotNull(response.getSearchResponse());
                    if (response.getSearchResponse().getSuccessfulShards() > 0) {
                        assertNotNull(response.getSearchResponse().getAggregations());
                        assertNotNull(response.getSearchResponse().getAggregations().get("terms"));
                        StringTerms terms = response.getSearchResponse().getAggregations().get("terms");
                        assertThat(terms.getBuckets().size(), greaterThanOrEqualTo(0));
                        assertThat(terms.getBuckets().size(), lessThanOrEqualTo(numKeywords));
                        for (InternalTerms.Bucket<?> bucket : terms.getBuckets()) {
                            long count = keywordFreqs.getOrDefault(bucket.getKeyAsString(), new AtomicInteger(0)).get();
                            assertThat(bucket.getDocCount(), lessThanOrEqualTo(count));
                        }
                    }
                }
                if (numFailures == numShards) {
                    assertNotNull(response.getFailure());
                } else {
                    assertNotNull(response.getSearchResponse());
                    assertNotNull(response.getSearchResponse().getAggregations());
                    assertNotNull(response.getSearchResponse().getAggregations().get("terms"));
                    StringTerms terms = response.getSearchResponse().getAggregations().get("terms");
                    assertThat(terms.getBuckets().size(), greaterThanOrEqualTo(0));
                    assertThat(terms.getBuckets().size(), lessThanOrEqualTo(numKeywords));
                    for (InternalTerms.Bucket<?> bucket : terms.getBuckets()) {
                        long count = keywordFreqs.getOrDefault(bucket.getKeyAsString(), new AtomicInteger(0)).get();
                        if (numFailures > 0) {
                            assertThat(bucket.getDocCount(), lessThanOrEqualTo(count));
                        } else {
                            assertThat(bucket.getDocCount(), equalTo(count));
                        }
                    }
                }
                deleteAsyncSearch(response.getId());
                ensureTaskRemoval(response.getId());
            } finally {
                response.decRef();
            }
        }
    }

    public void testRestartAfterCompletion() throws Exception {
        final String initialId;
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, new SearchSourceBuilder(), 0, 2)) {
            var initial = it.next();
            try {
                initialId = initial.getId();
            } finally {
                initial.decRef();
            }
            while (it.hasNext()) {
                it.next().decRef();
            }
        }
        ensureTaskCompletion(initialId);
        restartTaskNode(initialId, indexName);

        AsyncSearchResponse response = getAsyncSearch(initialId);
        try {
            assertNotNull(response.getSearchResponse());
            assertFalse(response.isRunning());
            assertFalse(response.isPartial());

            AsyncStatusResponse statusResponse = getAsyncStatus(initialId);
            assertFalse(statusResponse.isRunning());
            assertFalse(statusResponse.isPartial());
            assertEquals(numShards, statusResponse.getTotalShards());
            assertEquals(numShards, statusResponse.getSuccessfulShards());
            assertEquals(RestStatus.OK, statusResponse.getCompletionStatus());

            deleteAsyncSearch(response.getId());
            ensureTaskRemoval(response.getId());
        } finally {
            response.decRef();
        }
    }

    public void testDeleteCancelRunningTask() throws Exception {
        final AsyncSearchResponse initial;
        try (
            SearchResponseIterator it = assertBlockingIterator(indexName, numShards, new SearchSourceBuilder(), randomBoolean() ? 1 : 0, 2)
        ) {
            initial = it.next();
            initial.decRef();
            deleteAsyncSearch(initial.getId());
            it.close();
            ensureTaskCompletion(initial.getId());
            ensureTaskRemoval(initial.getId());
        }
    }

    public void testDeleteCleanupIndex() throws Exception {
        try (
            SearchResponseIterator it = assertBlockingIterator(indexName, numShards, new SearchSourceBuilder(), randomBoolean() ? 1 : 0, 2)
        ) {
            AsyncSearchResponse response = it.next();
            response.decRef();
            deleteAsyncSearch(response.getId());
            it.close();
            ensureTaskCompletion(response.getId());
            ensureTaskRemoval(response.getId());
        }
    }

    public void testCleanupOnFailure() throws Exception {
        final String initialId;
        try (SearchResponseIterator it = assertBlockingIterator(indexName, numShards, new SearchSourceBuilder(), numShards, 2)) {
            var resp = it.next();
            try {
                initialId = resp.getId();
            } finally {
                resp.decRef();
            }
        }
        ensureTaskCompletion(initialId);
        AsyncSearchResponse response = getAsyncSearch(initialId);
        try {
            assertFalse(response.isRunning());
            assertNotNull(response.getFailure());
            assertTrue(response.isPartial());
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getShardFailures().length, equalTo(numShards));
        } finally {
            response.decRef();
        }

        AsyncStatusResponse statusResponse = getAsyncStatus(initialId);
        assertFalse(statusResponse.isRunning());
        assertTrue(statusResponse.isPartial());
        assertEquals(numShards, statusResponse.getTotalShards());
        assertEquals(0, statusResponse.getSuccessfulShards());
        assertEquals(numShards, statusResponse.getFailedShards());
        assertThat(statusResponse.getCompletionStatus().getStatus(), greaterThanOrEqualTo(400));

        deleteAsyncSearch(initialId);
        ensureTaskRemoval(initialId);
    }

    public void testInvalidId() throws Exception {
        try (
            SearchResponseIterator it = assertBlockingIterator(indexName, numShards, new SearchSourceBuilder(), randomBoolean() ? 1 : 0, 2)
        ) {
            AsyncSearchResponse response = it.next();
            try {
                ExecutionException exc = expectThrows(ExecutionException.class, () -> getAsyncSearch("invalid"));
                assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
                assertThat(exc.getMessage(), containsString("invalid id"));
                while (it.hasNext()) {
                    response.decRef();
                    response = it.next();
                }
                assertFalse(response.isRunning());
            } finally {
                response.decRef();
            }
        }

        ExecutionException exc = expectThrows(ExecutionException.class, () -> getAsyncStatus("invalid"));
        assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exc.getMessage(), containsString("invalid id"));
    }

    public void testNoIndex() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest("invalid-*");
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        {
            final AsyncSearchResponse response = submitAsyncSearch(request);
            try {
                assertNotNull(response.getSearchResponse());
                assertFalse(response.isRunning());
                assertThat(response.getSearchResponse().getTotalShards(), equalTo(0));
            } finally {
                response.decRef();
            }
        }

        request = new SubmitAsyncSearchRequest("invalid");
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        {
            final var response = submitAsyncSearch(request);
            try {
                assertNull(response.getSearchResponse());
                assertNotNull(response.getFailure());
                assertFalse(response.isRunning());
                Exception exc = response.getFailure();
                assertThat(exc.getMessage(), containsString("error while executing search"));
                assertThat(exc.getCause().getMessage(), containsString("no such index"));
            } finally {
                response.decRef();
            }
        }
    }

    public void testCancellation() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        request.getSearchRequest().source(new SearchSourceBuilder().aggregation(new CancellingAggregationBuilder("test", randomLong())));
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        final String responseId;
        {
            final AsyncSearchResponse response = submitAsyncSearch(request);
            try {
                responseId = response.getId();
                assertNotNull(response.getSearchResponse());
                assertTrue(response.isRunning());
                assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
                assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
                assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
            } finally {
                response.decRef();
            }
        }

        {
            final var response = getAsyncSearch(responseId);
            try {
                assertNotNull(response.getSearchResponse());
                assertTrue(response.isRunning());
                assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
                assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
                assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));

                AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
                assertTrue(statusResponse.isRunning());
                assertEquals(numShards, statusResponse.getTotalShards());
                assertEquals(0, statusResponse.getSuccessfulShards());
                assertEquals(0, statusResponse.getSkippedShards());
                assertEquals(0, statusResponse.getFailedShards());

                deleteAsyncSearch(response.getId());
                ensureTaskRemoval(response.getId());
            } finally {
                response.decRef();
            }
        }
    }

    public void testUpdateRunningKeepAlive() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        request.getSearchRequest().source(new SearchSourceBuilder().aggregation(new CancellingAggregationBuilder("test", randomLong())));
        long now = System.currentTimeMillis();
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        final long expirationTime;
        final String responseId;
        {
            final AsyncSearchResponse response = submitAsyncSearch(request);
            try {
                responseId = response.getId();
                assertNotNull(response.getSearchResponse());
                assertTrue(response.isRunning());
                assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
                assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
                assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
                assertThat(response.getExpirationTime(), greaterThan(now));
                expirationTime = response.getExpirationTime();
            } finally {
                response.decRef();
            }
        }

        final String responseId2;
        {
            final AsyncSearchResponse response = getAsyncSearch(responseId);
            try {
                responseId2 = response.getId();
                assertNotNull(response.getSearchResponse());
                assertTrue(response.isRunning());
                assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
                assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
                assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
            } finally {
                response.decRef();
            }
        }

        final AsyncSearchResponse response = getAsyncSearch(responseId2, TimeValue.timeValueDays(10));
        try {
            assertThat(response.getExpirationTime(), greaterThan(expirationTime));

            assertTrue(response.isRunning());
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
            assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));

            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId(), TimeValue.timeValueDays(10));
            assertTrue(statusResponse.isRunning());
            assertTrue(statusResponse.isPartial());
            assertThat(statusResponse.getExpirationTime(), greaterThan(expirationTime));
            assertThat(statusResponse.getStartTime(), lessThan(statusResponse.getExpirationTime()));
            assertEquals(numShards, statusResponse.getTotalShards());
            assertEquals(0, statusResponse.getSuccessfulShards());
            assertEquals(0, statusResponse.getFailedShards());
            assertEquals(0, statusResponse.getSkippedShards());
            assertEquals(null, statusResponse.getCompletionStatus());
        } finally {
            response.decRef();
        }

        try {
            if (randomBoolean()) {
                final AsyncSearchResponse response2 = getAsyncSearch(response.getId(), TimeValue.timeValueMillis(1));
                try {
                    assertThat(response2.getExpirationTime(), lessThan(expirationTime));
                    ensureTaskNotRunning(response2.getId());
                    ensureTaskRemoval(response2.getId());
                } finally {
                    response2.decRef();
                }
            } else {
                try {
                    AsyncStatusResponse statusResponse = getAsyncStatus(response.getId(), TimeValue.timeValueMillis(5));
                    assertThat(statusResponse.getExpirationTime(), lessThan(expirationTime));
                } catch (ExecutionException e) {
                    Throwable cause = ExceptionsHelper.unwrap(e, ResourceNotFoundException.class);
                    // The 'get async search' method first updates the expiration time, then gets the response. So the
                    // maintenance service might remove the document right after it's updated, which means the get request
                    // fails with a 'not found' error. For now we allow this behavior, since it will be very rare in practice.
                    assertNotNull(
                        "ResourceNotFoundException is expected in some cases. Any other exception is not expected. Got: " + e,
                        cause
                    );
                }
            }
        } finally {
            ensureTaskNotRunning(response.getId());
            ensureTaskRemoval(response.getId());
        }
    }

    public void testUpdateStoreKeepAlive() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        long now = System.currentTimeMillis();
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(10));
        request.setKeepOnCompletion(true);
        AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertFalse(response.isRunning());
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
            assertThat(response.getExpirationTime(), greaterThan(now));

        } finally {
            response.decRef();
        }
        final String searchId = response.getId();
        long expirationTime = response.getExpirationTime();

        response = getAsyncSearch(searchId);
        try {
            assertNotNull(response.getSearchResponse());
            assertFalse(response.isRunning());
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
        } finally {
            response.decRef();
        }

        response = getAsyncSearch(searchId, TimeValue.timeValueDays(10));
        try {
            assertThat(response.getExpirationTime(), greaterThan(expirationTime));

            assertFalse(response.isRunning());
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
        } finally {
            response.decRef();
        }

        try {
            if (randomBoolean()) {
                AsyncSearchResponse finalResponse = getAsyncSearch(searchId, TimeValue.timeValueMillis(1));
                try {
                    assertThat(finalResponse.getExpirationTime(), lessThan(expirationTime));
                } finally {
                    finalResponse.decRef();
                }
            } else {
                AsyncStatusResponse statusResponse = getAsyncStatus(searchId, TimeValue.timeValueMillis(5));
                assertThat(statusResponse.getExpirationTime(), lessThan(expirationTime));
            }
        } catch (ExecutionException e) {
            // The 'get async search' method first updates the expiration time, then gets the response. So the
            // maintenance service might remove the document right after it's updated, which means the get request
            // fails with a 'not found' error. For now we allow this behavior, since it will be very rare in practice.
            if (ExceptionsHelper.unwrap(e, ResourceNotFoundException.class) == null) {
                throw e;
            }
        }

        ensureTaskNotRunning(searchId);
        ensureTaskRemoval(searchId);
    }

    public void testRemoveAsyncIndex() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(10));
        request.setKeepOnCompletion(true);
        long now = System.currentTimeMillis();

        final String responseId;
        final AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNotNull(response.getSearchResponse());
            assertFalse(response.isRunning());
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getFailedShards(), equalTo(0));
            assertThat(response.getExpirationTime(), greaterThan(now));
            responseId = response.getId();
        } finally {
            response.decRef();
        }

        // remove the async search index
        indicesAdmin().prepareDelete(XPackPlugin.ASYNC_RESULTS_INDEX).get();

        Exception exc = expectThrows(Exception.class, () -> getAsyncSearch(responseId));
        Throwable cause = exc instanceof ExecutionException
            ? ExceptionsHelper.unwrapCause(exc.getCause())
            : ExceptionsHelper.unwrapCause(exc);
        assertThat(ExceptionsHelper.status(cause).getStatus(), equalTo(404));

        SubmitAsyncSearchRequest newReq = new SubmitAsyncSearchRequest(indexName);
        newReq.getSearchRequest().source(new SearchSourceBuilder().aggregation(new CancellingAggregationBuilder("test", randomLong())));
        newReq.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1)).setKeepAlive(TimeValue.timeValueSeconds(1));
        final AsyncSearchResponse newResp = submitAsyncSearch(newReq);
        try {
            assertNotNull(newResp.getSearchResponse());
            assertTrue(newResp.isRunning());
            assertThat(newResp.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(newResp.getSearchResponse().getSuccessfulShards(), equalTo(0));
            assertThat(newResp.getSearchResponse().getFailedShards(), equalTo(0));

            // check garbage collection
            ensureTaskNotRunning(newResp.getId());
            ensureTaskRemoval(newResp.getId());
        } finally {
            newResp.decRef();
        }
    }

    public void testSearchPhaseFailure() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        request.setKeepOnCompletion(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(10));
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest()
            .source(new SearchSourceBuilder().query(new ThrowingQueryBuilder(randomLong(), new AlreadyClosedException("boom"), 0)));
        AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertFalse(response.isRunning());
            assertTrue(response.isPartial());
            assertThat(response.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertNotNull(response.getFailure());
            ensureTaskNotRunning(response.getId());
        } finally {
            response.decRef();
        }
    }

    public void testSearchPhaseFailureLeak() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        request.setKeepOnCompletion(true);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(10));
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest()
            .source(
                new SearchSourceBuilder().query(
                    new ThrowingQueryBuilder(randomLong(), new AlreadyClosedException("boom"), between(0, numShards - 1))
                )
            );
        request.getSearchRequest().source().aggregation(terms("f").field("f").size(between(1, 10)));

        AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertFalse(response.isRunning());
            assertTrue(response.isPartial());
            assertThat(response.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertNotNull(response.getFailure());
            ensureTaskNotRunning(response.getId());
        } finally {
            response.decRef();
        }
    }

    public void testMaxResponseSize() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder())
            .aggregation(AggregationBuilders.terms("terms").field("terms.keyword").size(numKeywords));

        final SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, indexName).setWaitForCompletionTimeout(
            TimeValue.timeValueSeconds(10)
        ).setKeepOnCompletion(true);

        int limit = 1000; // is not big enough to store the response
        updateClusterSettings(Settings.builder().put("search.max_async_search_response_size", limit + "b"));

        ExecutionException e = expectThrows(ExecutionException.class, () -> submitAsyncSearch(request));
        assertNotNull(e.getCause());
        assertThat(
            e.getMessage(),
            containsString(
                "Can't store an async search response larger than ["
                    + limit
                    + "] bytes. "
                    + "This limit can be set by changing the ["
                    + MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()
                    + "] setting."
            )
        );

        updateClusterSettings(Settings.builder().put("search.max_async_search_response_size", (String) null));
    }

    public void testCCSCheckCompatibility() throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchSourceBuilder().query(new DummyQueryBuilder() {
            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_CCS_VERSION, true);
            }
        }), indexName);

        AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertFalse(response.isRunning());
            Exception failure = response.getFailure();
            assertThat(failure.getMessage(), containsString("error while executing search"));
            assertThat(failure.getCause().getMessage(), containsString("the 'search.check_ccs_compatibility' setting is enabled"));
        } finally {
            response.decRef();
        }
    }
}
