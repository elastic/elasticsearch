/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.util;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchedDataIteratorTests extends ESTestCase {

    private static final String INDEX_NAME = "some_index_name";
    private static final String SCROLL_ID = "someScrollId";

    private Client client;
    private boolean wasScrollCleared;

    private TestIterator testIterator;

    private List<SearchRequest> searchRequestCaptor = new ArrayList<>();
    private List<SearchScrollRequest> searchScrollRequestCaptor = new ArrayList<>();

    @Before
    public void setUpMocks() {
        ThreadPool pool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(pool.getThreadContext()).thenReturn(threadContext);
        client = Mockito.mock(Client.class);
        when(client.threadPool()).thenReturn(pool);
        wasScrollCleared = false;
        testIterator = new TestIterator(client, INDEX_NAME);
        givenClearScrollRequest();
        searchRequestCaptor.clear();
        searchScrollRequestCaptor.clear();
    }

    public void testQueryReturnsNoResults() throws Exception {
        new ScrollResponsesMocker().finishMock();

        assertTrue(testIterator.hasNext());
        PlainActionFuture<Deque<String>> future = new PlainActionFuture<>();
        testIterator.next(future);
        assertTrue(future.get().isEmpty());
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);
        assertSearchRequest();
        assertSearchScrollRequests(0);
    }

    public void testCallingNextWhenHasNextIsFalseThrows() throws Exception {
        PlainActionFuture<Deque<String>> firstFuture = new PlainActionFuture<>();
        new ScrollResponsesMocker().addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c")).finishMock();
        testIterator.next(firstFuture);
        firstFuture.get();
        assertFalse(testIterator.hasNext());
        PlainActionFuture<Deque<String>> future = new PlainActionFuture<>();
        ExecutionException executionException = ESTestCase.expectThrows(ExecutionException.class, () -> {
            testIterator.next(future);
            future.get();
        });
        assertNotNull(executionException.getCause());
        assertTrue(executionException.getCause() instanceof NoSuchElementException);
    }

    public void testQueryReturnsSingleBatch() throws Exception {
        PlainActionFuture<Deque<String>> future = new PlainActionFuture<>();
        new ScrollResponsesMocker().addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c")).finishMock();

        assertTrue(testIterator.hasNext());
        testIterator.next(future);
        Deque<String> batch = future.get();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))));
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);

        assertSearchRequest();
        assertSearchScrollRequests(0);
    }

    public void testQueryReturnsThreeBatches() throws Exception {
        PlainActionFuture<Deque<String>> future = new PlainActionFuture<>();
        new ScrollResponsesMocker()
        .addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))
        .addBatch(createJsonDoc("d"), createJsonDoc("e"))
        .addBatch(createJsonDoc("f"))
        .finishMock();

        assertTrue(testIterator.hasNext());

        testIterator.next(future);
        Deque<String> batch = future.get();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))));

        future = new PlainActionFuture<>();
        testIterator.next(future);
        batch = future.get();
        assertEquals(2, batch.size());
        assertTrue(batch.containsAll(Arrays.asList(createJsonDoc("d"), createJsonDoc("e"))));

        future = new PlainActionFuture<>();
        testIterator.next(future);
        batch = future.get();
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

    @SuppressWarnings("unchecked")
    private void givenClearScrollRequest() {
        ClearScrollRequestBuilder requestBuilder = mock(ClearScrollRequestBuilder.class);

        when(client.prepareClearScroll()).thenReturn(requestBuilder);
        when(requestBuilder.setScrollIds(Collections.singletonList(SCROLL_ID))).thenReturn(requestBuilder);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(SCROLL_ID);
        when(requestBuilder.request()).thenReturn(clearScrollRequest);
        doAnswer((answer) -> {
            wasScrollCleared = true;
            ActionListener<ClearScrollResponse> scrollListener =
                (ActionListener<ClearScrollResponse>) answer.getArguments()[1];
            scrollListener.onResponse(new ClearScrollResponse(true,0));
            return null;
        }).when(client).clearScroll(any(ClearScrollRequest.class), any(ActionListener.class));
    }

    private void assertSearchRequest() {
        List<SearchRequest> searchRequests = searchRequestCaptor;
        assertThat(searchRequests.size(), equalTo(1));
        SearchRequest searchRequest = searchRequests.get(0);
        assertThat(searchRequest.indices(), equalTo(new String[] {INDEX_NAME}));
        assertThat(searchRequest.scroll().keepAlive(), equalTo(TimeValue.timeValueMinutes(5)));
        assertThat(searchRequest.types().length, equalTo(0));
        assertThat(searchRequest.source().query(), equalTo(QueryBuilders.matchAllQuery()));
        assertThat(searchRequest.source().trackTotalHitsUpTo(), is(SearchContext.TRACK_TOTAL_HITS_ACCURATE));
    }

    private void assertSearchScrollRequests(int expectedCount) {
        List<SearchScrollRequest> searchScrollRequests = searchScrollRequestCaptor;
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

        @SuppressWarnings("unchecked")
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
                SearchResponse first = responses.get(0);
                if (responses.size() > 1) {
                    List<SearchResponse> rest = new ArrayList<>(responses);
                    Iterator<SearchResponse> responseIterator = rest.iterator();
                    doAnswer((answer) -> {
                        SearchScrollRequest request = (SearchScrollRequest)answer.getArguments()[0];
                        ActionListener<SearchResponse> rsp = (ActionListener<SearchResponse>)answer.getArguments()[1];
                        searchScrollRequestCaptor.add(request);
                        rsp.onResponse(responseIterator.next());
                        return null;
                    }).when(client).searchScroll(any(SearchScrollRequest.class), any(ActionListener.class));
                } else {
                    doAnswer((answer) -> {
                        SearchScrollRequest request = (SearchScrollRequest)answer.getArguments()[0];
                        ActionListener<SearchResponse> rsp = (ActionListener<SearchResponse>)answer.getArguments()[1];
                        searchScrollRequestCaptor.add(request);
                        rsp.onResponse(first);
                        return null;
                    }).when(client).searchScroll(any(SearchScrollRequest.class), any(ActionListener.class));
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void givenInitialResponse(String... hits) {
            SearchResponse searchResponse = createSearchResponseWithHits(hits);
            doAnswer((answer) -> {
                SearchRequest request = (SearchRequest)answer.getArguments()[0];
                searchRequestCaptor.add(request);
                ActionListener<SearchResponse> rsp = (ActionListener<SearchResponse>)answer.getArguments()[1];
                rsp.onResponse(searchResponse);
                return null;
            }).when(client).search(any(SearchRequest.class), any(ActionListener.class));
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

    private static class TestIterator extends BatchedDataIterator<String, Deque<String>> {
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

        @Override
        protected Deque<String> getCollection() {
            return new ArrayDeque<>();
        }

        @Override
        protected SortOrder sortOrder() {
            return SortOrder.DESC;
        }

        @Override
        protected String sortField() {
            return "foo";
        }
    }
    public class SearchHitBuilder {

        private final SearchHit hit;
        private final Map<String, DocumentField> fields;

        public SearchHitBuilder(int docId) {
            hit = new SearchHit(docId);
            fields = new HashMap<>();
        }

        public SearchHitBuilder setSource(String sourceJson) {
            hit.sourceRef(new BytesArray(sourceJson));
            return this;
        }

        public SearchHit build() {
            if (!fields.isEmpty()) {
                hit.fields(fields);
            }
            return hit;
        }
    }
}
