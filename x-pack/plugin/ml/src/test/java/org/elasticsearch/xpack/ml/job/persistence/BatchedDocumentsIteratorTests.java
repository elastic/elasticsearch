/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchedDocumentsIteratorTests extends ESTestCase {

    private static final String INDEX_NAME = ".ml-anomalies-foo";
    private static final String SCROLL_ID = "someScrollId";

    private Client client;
    private boolean wasScrollCleared;

    private TestIterator testIterator;

    private ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    private ArgumentCaptor<SearchScrollRequest> searchScrollRequestCaptor = ArgumentCaptor.forClass(SearchScrollRequest.class);

    @Before
    public void setUpMocks() {
        client = Mockito.mock(Client.class);
        wasScrollCleared = false;
        testIterator = new TestIterator(client, INDEX_NAME);
        givenClearScrollRequest();
    }

    public void testQueryReturnsNoResults() {
        new ScrollResponsesMocker().finishMock();

        assertTrue(testIterator.hasNext());
        assertTrue(testIterator.next().isEmpty());
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);
        assertSearchRequest();
        assertSearchScrollRequests(0);
    }

    public void testCallingNextWhenHasNextIsFalseThrows() {
        new ScrollResponsesMocker().addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c")).finishMock();
        testIterator.next();
        assertFalse(testIterator.hasNext());

        ESTestCase.expectThrows(NoSuchElementException.class, () -> testIterator.next());
    }

    public void testQueryReturnsSingleBatch() {
        new ScrollResponsesMocker().addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c")).finishMock();

        assertTrue(testIterator.hasNext());
        Deque<String> batch = testIterator.next();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))));
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);

        assertSearchRequest();
        assertSearchScrollRequests(0);
    }

    public void testQueryReturnsThreeBatches() {
        new ScrollResponsesMocker()
        .addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))
        .addBatch(createJsonDoc("d"), createJsonDoc("e"))
        .addBatch(createJsonDoc("f"))
        .finishMock();

        assertTrue(testIterator.hasNext());

        Deque<String> batch = testIterator.next();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))));

        batch = testIterator.next();
        assertEquals(2, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("d"), createJsonDoc("e"))));

        batch = testIterator.next();
        assertEquals(1, batch.size());
        assertTrue(batch.containsAll(Collections.singletonList(createJsonDoc("f"))));

        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);

        assertSearchRequest();
        assertSearchScrollRequests(2);
    }

    private String createJsonDoc(String value) {
        return "{\"foo\":\"" + value + "\"}";
    }

    private void givenClearScrollRequest() {
        ClearScrollRequestBuilder requestBuilder = mock(ClearScrollRequestBuilder.class);
        when(client.prepareClearScroll()).thenReturn(requestBuilder);
        when(requestBuilder.setScrollIds(Collections.singletonList(SCROLL_ID))).thenReturn(requestBuilder);
        when(requestBuilder.get()).thenAnswer((invocation) -> {
            wasScrollCleared = true;
            return null;
        });
    }

    private void assertSearchRequest() {
        List<SearchRequest> searchRequests = searchRequestCaptor.getAllValues();
        assertThat(searchRequests.size(), equalTo(1));
        SearchRequest searchRequest = searchRequests.get(0);
        assertThat(searchRequest.indices(), equalTo(new String[] {INDEX_NAME}));
        assertThat(searchRequest.scroll().keepAlive(), equalTo(TimeValue.timeValueMinutes(5)));
        assertThat(searchRequest.source().query(), equalTo(QueryBuilders.matchAllQuery()));
        assertThat(searchRequest.source().trackTotalHitsUpTo(), is(SearchContext.TRACK_TOTAL_HITS_ACCURATE));
    }

    private void assertSearchScrollRequests(int expectedCount) {
        List<SearchScrollRequest> searchScrollRequests = searchScrollRequestCaptor.getAllValues();
        assertThat(searchScrollRequests.size(), equalTo(expectedCount));
        for (SearchScrollRequest request : searchScrollRequests) {
            assertThat(request.scrollId(), equalTo(SCROLL_ID));
            assertThat(request.scroll().keepAlive(), equalTo(TimeValue.timeValueMinutes(5)));
        }
    }

    private class ScrollResponsesMocker {
        private List<String[]> batches = new ArrayList<>();
        private long totalHits = 0;
        private List<SearchResponse> responses = new ArrayList<>();

        ScrollResponsesMocker addBatch(String... hits) {
            totalHits += hits.length;
            batches.add(hits);
            return this;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void finishMock() {
            if (batches.isEmpty()) {
                givenInitialResponse();
                return;
            }
            givenInitialResponse(batches.get(0));
            for (int i = 1; i < batches.size(); ++i) {
                givenNextResponse(batches.get(i));
            }
            if (responses.size() > 0) {
                ActionFuture<SearchResponse> first = wrapResponse(responses.get(0));
                if (responses.size() > 1) {
                    List<ActionFuture<SearchResponse>> rest = new ArrayList<>();
                    for (int i = 1; i < responses.size(); ++i) {
                        rest.add(wrapResponse(responses.get(i)));
                    }

                    when(client.searchScroll(searchScrollRequestCaptor.capture())).thenReturn(
                            first, rest.toArray(new ActionFuture[rest.size() - 1]));
                } else {
                    when(client.searchScroll(searchScrollRequestCaptor.capture())).thenReturn(first);
                }
            }
        }

        private void givenInitialResponse(String... hits) {
            SearchResponse searchResponse = createSearchResponseWithHits(hits);
            ActionFuture<SearchResponse> future = wrapResponse(searchResponse);
            when(future.actionGet()).thenReturn(searchResponse);
            when(client.search(searchRequestCaptor.capture())).thenReturn(future);
        }

        @SuppressWarnings("unchecked")
        private ActionFuture<SearchResponse> wrapResponse(SearchResponse searchResponse) {
            ActionFuture<SearchResponse> future = mock(ActionFuture.class);
            when(future.actionGet()).thenReturn(searchResponse);
            return future;
        }

        private void givenNextResponse(String... hits) {
            responses.add(createSearchResponseWithHits(hits));
        }

        private SearchResponse createSearchResponseWithHits(String... hits) {
            SearchHits searchHits = createHits(hits);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getScrollId()).thenReturn(SCROLL_ID);
            when(searchResponse.getHits()).thenReturn(searchHits);
            return searchResponse;
        }

        private SearchHits createHits(String... values) {
            List<SearchHit> hits = new ArrayList<>();
            for (String value : values) {
                hits.add(new SearchHitBuilder(randomInt()).setSource(value).build());
            }
            return new SearchHits(hits.toArray(new SearchHit[hits.size()]), new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0f);
        }
    }

    private static class TestIterator extends BatchedDocumentsIterator<String> {
        TestIterator(Client client, String jobId) {
            super(client, jobId);
        }

        @Override
        protected QueryBuilder getQuery() {
            return QueryBuilders.matchAllQuery();
        }

        @Override
        protected String map(SearchHit hit) {
            return hit.getSourceAsString();
        }
    }
}
