/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils.persistence;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class BatchedDocumentsIteratorTests extends ESTestCase {

    private static final String INDEX_NAME = ".ml-anomalies-foo";
    private static final String SCROLL_ID = "someScrollId";

    private Client client;
    private boolean wasScrollCleared;

    private TestIterator testIterator;

    @Before
    public void setUpMocks() {
        client = Mockito.mock(Client.class);
        OriginSettingClient originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        wasScrollCleared = false;
        testIterator = new TestIterator(originSettingClient, INDEX_NAME);
        givenClearScrollRequest();
    }

    public void testQueryReturnsNoResults() {
        ResponsesMocker scrollResponsesMocker = new ScrollResponsesMocker(client).finishMock();

        assertTrue(testIterator.hasNext());
        assertTrue(testIterator.next().isEmpty());
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);
        scrollResponsesMocker.assertSearchRequest(INDEX_NAME);
        scrollResponsesMocker.assertSearchScrollRequests(0, SCROLL_ID);
    }

    public void testCallingNextWhenHasNextIsFalseThrows() {
        new ScrollResponsesMocker(client)
            .addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))
            .finishMock();
        testIterator.next();
        assertFalse(testIterator.hasNext());

        ESTestCase.expectThrows(NoSuchElementException.class, () -> testIterator.next());
    }

    public void testQueryReturnsSingleBatch() {
        ResponsesMocker scrollResponsesMocker = new ScrollResponsesMocker(client)
            .addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))
            .finishMock();

        assertTrue(testIterator.hasNext());
        Deque<String> batch = testIterator.next();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))));
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);

        scrollResponsesMocker.assertSearchRequest(INDEX_NAME);
        scrollResponsesMocker.assertSearchScrollRequests(0, SCROLL_ID);
    }

    public void testQueryReturnsThreeBatches() {
        ResponsesMocker responsesMocker = new ScrollResponsesMocker(client)
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

        responsesMocker.assertSearchRequest(INDEX_NAME);
        responsesMocker.assertSearchScrollRequests(2, SCROLL_ID);
    }

    private String createJsonDoc(String value) {
        return "{\"foo\":\"" + value + "\"}";
    }

    @SuppressWarnings("unchecked")
    private void givenClearScrollRequest() {
        doAnswer(invocationOnMock -> {
            ActionListener<ClearScrollResponse> listener = (ActionListener<ClearScrollResponse>) invocationOnMock.getArguments()[2];
            wasScrollCleared = true;
            listener.onResponse(mock(ClearScrollResponse.class));
            return null;
        }).when(client).execute(eq(ClearScrollAction.INSTANCE), any(), any());
    }


    abstract static class ResponsesMocker {
        protected Client client;
        protected List<String[]> batches = new ArrayList<>();
        protected long totalHits = 0;
        protected List<SearchResponse> responses = new ArrayList<>();

        protected AtomicInteger responseIndex = new AtomicInteger(0);

        protected ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        protected ArgumentCaptor<SearchScrollRequest> searchScrollRequestCaptor = ArgumentCaptor.forClass(SearchScrollRequest.class);

        ResponsesMocker(Client client) {
            this.client = client;
        }

        ResponsesMocker addBatch(String... hits) {
            totalHits += hits.length;
            batches.add(hits);
            return this;
        }

        abstract ResponsesMocker finishMock();


        protected SearchResponse createSearchResponseWithHits(String... hits) {
            SearchHits searchHits = createHits(hits);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getScrollId()).thenReturn(SCROLL_ID);
            when(searchResponse.getHits()).thenReturn(searchHits);
            return searchResponse;
        }

        protected SearchHits createHits(String... values) {
            List<SearchHit> hits = new ArrayList<>();
            for (String value : values) {
                hits.add(new SearchHitBuilder(randomInt()).setSource(value).build());
            }
            return new SearchHits(hits.toArray(new SearchHit[hits.size()]), new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0f);
        }

        void assertSearchRequest(String indexName) {
            List<SearchRequest> searchRequests = searchRequestCaptor.getAllValues();
            assertThat(searchRequests.size(), equalTo(1));
            SearchRequest searchRequest = searchRequests.get(0);
            assertThat(searchRequest.indices(), equalTo(new String[] {indexName}));
            assertThat(searchRequest.scroll().keepAlive(), equalTo(TimeValue.timeValueMinutes(5)));
            assertThat(searchRequest.source().query(), equalTo(QueryBuilders.matchAllQuery()));
            assertThat(searchRequest.source().trackTotalHitsUpTo(), is(SearchContext.TRACK_TOTAL_HITS_ACCURATE));
        }

        void assertSearchScrollRequests(int expectedCount, String scrollId) {
            List<SearchScrollRequest> searchScrollRequests = searchScrollRequestCaptor.getAllValues();
            assertThat(searchScrollRequests.size(), equalTo(expectedCount));
            for (SearchScrollRequest request : searchScrollRequests) {
                assertThat(request.scrollId(), equalTo(scrollId));
                assertThat(request.scroll().keepAlive(), equalTo(TimeValue.timeValueMinutes(5)));
            }
        }
    }

    static class ScrollResponsesMocker extends ResponsesMocker {

        ScrollResponsesMocker(Client client) {
            super(client);
        }

        @Override
        @SuppressWarnings("unchecked")
        ResponsesMocker finishMock()
        {
            if (batches.isEmpty()) {
                givenInitialResponse();
                return this;
            }

            givenInitialResponse(batches.get(0));
            for (int i = 1; i < batches.size(); ++i) {
                responses.add(createSearchResponseWithHits(batches.get(i)));
            }

            doAnswer(invocationOnMock -> {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(responses.get(responseIndex.getAndIncrement()));
                return null;
            }).when(client).execute(eq(SearchScrollAction.INSTANCE), searchScrollRequestCaptor.capture(), any());

            return this;
        }

        @SuppressWarnings("unchecked")
        private void givenInitialResponse(String... hits) {
            SearchResponse searchResponse = createSearchResponseWithHits(hits);

            doAnswer(invocationOnMock -> {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(searchResponse);
                return null;
            }).when(client).execute(eq(SearchAction.INSTANCE), searchRequestCaptor.capture(), any());
        }
    }

    static class SearchResponsesMocker extends ResponsesMocker {

        SearchResponsesMocker(Client client) {
            super(client);
        }

        @Override
        @SuppressWarnings("unchecked")
        ResponsesMocker finishMock()
        {
            if (batches.isEmpty()) {
                doAnswer(invocationOnMock -> {
                    ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
                    listener.onResponse(createSearchResponseWithHits());
                    return null;
                }).when(client).execute(eq(SearchAction.INSTANCE), searchRequestCaptor.capture(), any());

                return this;
            }

            for (String[] batch : batches) {
                responses.add(createSearchResponseWithHits(batch));
            }

            doAnswer(invocationOnMock -> {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(responses.get(responseIndex.getAndIncrement()));
                return null;
            }).when(client).execute(eq(SearchAction.INSTANCE), searchRequestCaptor.capture(), any());

            return this;
        }
    }


    private static class TestIterator extends BatchedDocumentsIterator<String> {
        TestIterator(OriginSettingClient client, String jobId) {
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
