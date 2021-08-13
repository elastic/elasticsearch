/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Deque;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SearchAfterDocumentsIteratorTests extends ESTestCase {

    private static final String INDEX_NAME = "test-index";
    private Client client;
    private OriginSettingClient originSettingClient;

    @Before
    public void setUpMocks() {
        client = Mockito.mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
    }

    public void testHasNext()
    {
        new BatchedDocumentsIteratorTests.SearchResponsesMocker(client)
            .addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))
            .addBatch(createJsonDoc("d"), createJsonDoc("e"))
            .finishMock();

        TestIterator testIterator = new TestIterator(originSettingClient, INDEX_NAME);
        testIterator.setBatchSize(3);
        assertTrue(testIterator.hasNext());
        Deque<String> batch = testIterator.next();
        assertThat(batch, hasSize(3));

        assertTrue(testIterator.hasNext());
        batch = testIterator.next();
        assertThat(batch, hasSize(2));

        assertFalse(testIterator.hasNext());
        ESTestCase.expectThrows(NoSuchElementException.class, testIterator::next);
    }

    public void testFirstBatchIsEmpty()
    {
        new BatchedDocumentsIteratorTests.SearchResponsesMocker(client)
            .addBatch()
            .finishMock();

        TestIterator testIterator = new TestIterator(originSettingClient, INDEX_NAME);
        assertTrue(testIterator.hasNext());
        Deque<String> next = testIterator.next();
        assertThat(next, empty());
        assertFalse(testIterator.hasNext());
    }

    public void testExtractSearchAfterValuesSet()
    {
        new BatchedDocumentsIteratorTests.SearchResponsesMocker(client)
            .addBatch(createJsonDoc("a"), createJsonDoc("b"), createJsonDoc("c"))
            .addBatch(createJsonDoc("d"), createJsonDoc("e"))
            .finishMock();

        TestIterator testIterator = new TestIterator(originSettingClient, INDEX_NAME);
        testIterator.setBatchSize(3);
        Deque<String> next = testIterator.next();
        assertThat(next, not(empty()));
        Object[] values = testIterator.searchAfterFields();
        assertArrayEquals(new Object[] {"c"}, values);

        next = testIterator.next();
        assertThat(next, not(empty()));
        values = testIterator.searchAfterFields();
        assertArrayEquals(new Object[] {"e"}, values);
    }

    private static class TestIterator extends SearchAfterDocumentsIterator<String> {

        private String searchAfterValue;

        TestIterator(OriginSettingClient client, String index) {
            super(client, index);
        }

        @Override
        protected QueryBuilder getQuery() {
            return QueryBuilders.matchAllQuery();
        }

        @Override
        protected FieldSortBuilder sortField() {
            return new FieldSortBuilder("name");
        }

        @Override
        protected String map(SearchHit hit) {
            return hit.getSourceAsString();
        }

        @Override
        protected Object[] searchAfterFields() {
            return new Object[] {searchAfterValue};
        }

        @Override
        protected void extractSearchAfterFields(SearchHit lastSearchHit) {
            searchAfterValue = (String)lastSearchHit.getSourceAsMap().get("name");
        }
    }

    private String createJsonDoc(String value) {
        return "{\"name\":\"" + value + "\"}";
    }
}
