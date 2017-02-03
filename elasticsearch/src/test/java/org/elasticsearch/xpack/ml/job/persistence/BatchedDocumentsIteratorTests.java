/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/prelert-legacy/issues/127")
public class BatchedDocumentsIteratorTests extends ESTestCase {
    private static final String INDEX_NAME = ".ml-anomalies-foo";
    private static final String SCROLL_ID = "someScrollId";

    private Client client;
    private boolean wasScrollCleared;

    private TestIterator testIterator;

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
    }

    public void testCallingNextWhenHasNextIsFalseThrows() {
        new ScrollResponsesMocker().addBatch("a", "b", "c").finishMock();
        testIterator.next();
        assertFalse(testIterator.hasNext());

        ESTestCase.expectThrows(NoSuchElementException.class, () -> testIterator.next());
    }

    public void testQueryReturnsSingleBatch() {
        new ScrollResponsesMocker().addBatch("a", "b", "c").finishMock();

        assertTrue(testIterator.hasNext());
        Deque<String> batch = testIterator.next();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList("a", "b", "c")));
        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);
    }

    public void testQueryReturnsThreeBatches() {
        new ScrollResponsesMocker()
        .addBatch("a", "b", "c")
        .addBatch("d", "e")
        .addBatch("f")
        .finishMock();

        assertTrue(testIterator.hasNext());

        Deque<String> batch = testIterator.next();
        assertEquals(3, batch.size());
        assertTrue(batch.containsAll(Arrays.asList("a", "b", "c")));

        batch = testIterator.next();
        assertEquals(2, batch.size());
        assertTrue(batch.containsAll(Arrays.asList("d", "e")));

        batch = testIterator.next();
        assertEquals(1, batch.size());
        assertTrue(batch.containsAll(Arrays.asList("f")));

        assertFalse(testIterator.hasNext());
        assertTrue(wasScrollCleared);
    }

    private void givenClearScrollRequest() {
        ClearScrollRequestBuilder requestBuilder = mock(ClearScrollRequestBuilder.class);
        when(client.prepareClearScroll()).thenReturn(requestBuilder);
        when(requestBuilder.setScrollIds(Arrays.asList(SCROLL_ID))).thenReturn(requestBuilder);
        when(requestBuilder.get()).thenAnswer((invocation) -> {
            wasScrollCleared = true;
            return null;
        });
    }

    private class ScrollResponsesMocker {
        private List<String[]> batches = new ArrayList<>();
        private long totalHits = 0;
        private List<SearchScrollRequestBuilder> nextRequestBuilders = new ArrayList<>();

        ScrollResponsesMocker addBatch(String... hits) {
            totalHits += hits.length;
            batches.add(hits);
            return this;
        }

        void finishMock() {
            if (batches.isEmpty()) {
                givenInitialResponse();
                return;
            }
            givenInitialResponse(batches.get(0));
            for (int i = 1; i < batches.size(); ++i) {
                givenNextResponse(batches.get(i));
            }
            if (nextRequestBuilders.size() > 0) {
                SearchScrollRequestBuilder first = nextRequestBuilders.get(0);
                if (nextRequestBuilders.size() > 1) {
                    SearchScrollRequestBuilder[] rest = new SearchScrollRequestBuilder[batches.size() - 1];
                    for (int i = 1; i < nextRequestBuilders.size(); ++i) {
                        rest[i - 1] = nextRequestBuilders.get(i);
                    }
                    when(client.prepareSearchScroll(SCROLL_ID)).thenReturn(first, rest);
                } else {
                    when(client.prepareSearchScroll(SCROLL_ID)).thenReturn(first);
                }
            }
        }

        private void givenInitialResponse(String... hits) {
            SearchResponse searchResponse = createSearchResponseWithHits(hits);
            SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
            when(client.prepareSearch(INDEX_NAME)).thenReturn(requestBuilder);
            when(requestBuilder.setScroll("5m")).thenReturn(requestBuilder);
            when(requestBuilder.setSize(10000)).thenReturn(requestBuilder);
            when(requestBuilder.setTypes("String")).thenReturn(requestBuilder);
            when(requestBuilder.setQuery(any(QueryBuilder.class))).thenReturn(requestBuilder);
            when(requestBuilder.addSort(any(SortBuilder.class))).thenReturn(requestBuilder);
            when(requestBuilder.get()).thenReturn(searchResponse);
        }

        private void givenNextResponse(String... hits) {
            SearchResponse searchResponse = createSearchResponseWithHits(hits);
            SearchScrollRequestBuilder requestBuilder = mock(SearchScrollRequestBuilder.class);
            when(requestBuilder.setScrollId(SCROLL_ID)).thenReturn(requestBuilder);
            when(requestBuilder.setScroll("5m")).thenReturn(requestBuilder);
            when(requestBuilder.get()).thenReturn(searchResponse);
            nextRequestBuilders.add(requestBuilder);
        }

        private SearchResponse createSearchResponseWithHits(String... hits) {
            SearchHits searchHits = createHits(hits);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getScrollId()).thenReturn(SCROLL_ID);
            when(searchResponse.getHits()).thenReturn(searchHits);
            return searchResponse;
        }

        private SearchHits createHits(String... values) {
            SearchHits searchHits = mock(SearchHits.class);
            List<SearchHit> hits = new ArrayList<>();
            for (String value : values) {
                SearchHit hit = mock(SearchHit.class);
                when(hit.getSourceAsString()).thenReturn(value);
                hits.add(hit);
            }
            when(searchHits.getTotalHits()).thenReturn(totalHits);
            when(searchHits.getHits()).thenReturn(hits.toArray(new SearchHit[hits.size()]));
            return searchHits;
        }
    }

    private static class TestIterator extends BatchedDocumentsIterator<String> {
        TestIterator(Client client, String jobId) {
            super(client, jobId);
        }

        @Override
        protected String getType() {
            return "String";
        }

        @Override
        protected String map(SearchHit hit) {
            return hit.getSourceAsString();
        }
    }

}
