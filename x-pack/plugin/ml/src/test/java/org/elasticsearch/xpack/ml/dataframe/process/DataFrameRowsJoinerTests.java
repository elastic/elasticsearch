/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DataFrameRowsJoinerTests extends ESTestCase {

    private static final String ANALYTICS_ID = "my_analytics";

    private Client client;
    private DataFrameDataExtractor dataExtractor;
    private ArgumentCaptor<BulkRequest> bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        dataExtractor = mock(DataFrameDataExtractor.class);
    }

    public void testProcess_GivenNoResults() {
        givenProcessResults(Collections.emptyList());
        verifyNoMoreInteractions(client);
    }

    public void testProcess_GivenSingleRowAndResult() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row row = newRow(newHit(dataDoc), dataValues, 1);
        givenDataFrameBatches(List.of(Arrays.asList(row)));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        RowResults result = new RowResults(1, resultFields);
        givenProcessResults(Arrays.asList(result));

        List<BulkRequest> capturedBulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(capturedBulkRequests.size(), equalTo(1));
        BulkRequest capturedBulkRequest = capturedBulkRequests.get(0);
        assertThat(capturedBulkRequest.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) capturedBulkRequest.requests().get(0);
        Map<String, Object> indexedDocSource = indexRequest.sourceAsMap();
        assertThat(indexedDocSource.size(), equalTo(4));
        assertThat(indexedDocSource.get("f_1"), equalTo("foo"));
        assertThat(indexedDocSource.get("f_2"), equalTo(42.0));
        assertThat(indexedDocSource.get("a"), equalTo("1"));
        assertThat(indexedDocSource.get("b"), equalTo("2"));
    }

    public void testProcess_GivenFullResultsBatch() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        List<DataFrameDataExtractor.Row> firstBatch = new ArrayList<>(1000);
        IntStream.range(0, 1000).forEach(i -> firstBatch.add(newRow(newHit(dataDoc), dataValues, i)));
        List<DataFrameDataExtractor.Row> secondBatch = new ArrayList<>(1);
        secondBatch.add(newRow(newHit(dataDoc), dataValues, 1000));
        givenDataFrameBatches(List.of(firstBatch, secondBatch));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        List<RowResults> results = new ArrayList<>(1001);
        IntStream.range(0, 1001).forEach(i -> results.add(new RowResults(i, resultFields)));
        givenProcessResults(results);

        List<BulkRequest> capturedBulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(capturedBulkRequests.size(), equalTo(2));
        assertThat(capturedBulkRequests.get(0).numberOfActions(), equalTo(1000));
        assertThat(capturedBulkRequests.get(1).numberOfActions(), equalTo(1));
    }

    public void testProcess_GivenSingleRowAndResultWithMismatchingIdHash() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row row = newRow(newHit(dataDoc), dataValues, 1);
        givenDataFrameBatches(List.of(Arrays.asList(row)));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        RowResults result = new RowResults(2, resultFields);
        givenProcessResults(Arrays.asList(result));

        verifyNoMoreInteractions(client);
    }

    public void testProcess_GivenSingleBatchWithSkippedRows() throws IOException {
        givenClientHasNoFailures();

        DataFrameDataExtractor.Row skippedRow = newRow(newHit("{}"), null, 1);
        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row normalRow = newRow(newHit(dataDoc), dataValues, 2);
        givenDataFrameBatches(List.of(Arrays.asList(skippedRow, normalRow)));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        RowResults result = new RowResults(2, resultFields);
        givenProcessResults(Arrays.asList(result));

        List<BulkRequest> capturedBulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(capturedBulkRequests.size(), equalTo(1));
        BulkRequest capturedBulkRequest = capturedBulkRequests.get(0);
        assertThat(capturedBulkRequest.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) capturedBulkRequest.requests().get(0);
        Map<String, Object> indexedDocSource = indexRequest.sourceAsMap();
        assertThat(indexedDocSource.size(), equalTo(4));
        assertThat(indexedDocSource.get("f_1"), equalTo("foo"));
        assertThat(indexedDocSource.get("f_2"), equalTo(42.0));
        assertThat(indexedDocSource.get("a"), equalTo("1"));
        assertThat(indexedDocSource.get("b"), equalTo("2"));
    }

    public void testProcess_GivenTwoBatchesWhereFirstEndsWithSkippedRow() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row normalRow1 = newRow(newHit(dataDoc), dataValues, 1);
        DataFrameDataExtractor.Row normalRow2 = newRow(newHit(dataDoc), dataValues, 2);
        DataFrameDataExtractor.Row skippedRow = newRow(newHit("{}"), null, 3);
        DataFrameDataExtractor.Row normalRow3 = newRow(newHit(dataDoc), dataValues, 4);
        givenDataFrameBatches(List.of(Arrays.asList(normalRow1, normalRow2, skippedRow), Arrays.asList(normalRow3)));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        RowResults result1 = new RowResults(1, resultFields);
        RowResults result2 = new RowResults(2, resultFields);
        RowResults result3 = new RowResults(4, resultFields);
        givenProcessResults(Arrays.asList(result1, result2, result3));

        List<BulkRequest> capturedBulkRequests = bulkRequestCaptor.getAllValues();
        assertThat(capturedBulkRequests.size(), equalTo(1));
        BulkRequest capturedBulkRequest = capturedBulkRequests.get(0);
        assertThat(capturedBulkRequest.numberOfActions(), equalTo(3));
        IndexRequest indexRequest = (IndexRequest) capturedBulkRequest.requests().get(0);
        Map<String, Object> indexedDocSource = indexRequest.sourceAsMap();
        assertThat(indexedDocSource.size(), equalTo(4));
        assertThat(indexedDocSource.get("f_1"), equalTo("foo"));
        assertThat(indexedDocSource.get("f_2"), equalTo(42.0));
        assertThat(indexedDocSource.get("a"), equalTo("1"));
        assertThat(indexedDocSource.get("b"), equalTo("2"));
    }

    public void testProcess_GivenMoreResultsThanRows() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row row = newRow(newHit(dataDoc), dataValues, 1);
        givenDataFrameBatches(List.of(List.of(row)));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        RowResults result1 = new RowResults(1, resultFields);
        RowResults result2 = new RowResults(2, resultFields);
        givenProcessResults(Arrays.asList(result1, result2));

        verifyNoMoreInteractions(client);
    }

    public void testProcess_GivenNoResults_ShouldCancelAndConsumeExtractor() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row row1 = newRow(newHit(dataDoc), dataValues, 1);
        DataFrameDataExtractor.Row row2 = newRow(newHit(dataDoc), dataValues, 1);
        givenDataFrameBatches(List.of(List.of(row1), List.of(row2)));

        givenProcessResults(Collections.emptyList());

        verifyNoMoreInteractions(client);
        verify(dataExtractor).cancel();
        verify(dataExtractor, times(2)).next();
    }

    private void givenProcessResults(List<RowResults> results) {
        try (DataFrameRowsJoiner joiner = new DataFrameRowsJoiner(ANALYTICS_ID, client, dataExtractor)) {
            results.forEach(joiner::processRowResults);
        }
    }

    private void givenDataFrameBatches(List<List<DataFrameDataExtractor.Row>> batches) throws IOException {
        DelegateStubDataExtractor delegateStubDataExtractor = new DelegateStubDataExtractor(batches);
        when(dataExtractor.hasNext()).thenAnswer(a -> delegateStubDataExtractor.hasNext());
        when(dataExtractor.next()).thenAnswer(a -> delegateStubDataExtractor.next());
    }

    private static SearchHit newHit(String json) {
        SearchHit hit = new SearchHit(randomInt(), randomAlphaOfLength(10), Collections.emptyMap());
        hit.sourceRef(new BytesArray(json));
        return hit;
    }

    private static DataFrameDataExtractor.Row newRow(SearchHit hit, String[] values, int checksum) {
        DataFrameDataExtractor.Row row = mock(DataFrameDataExtractor.Row.class);
        when(row.getHit()).thenReturn(hit);
        when(row.getValues()).thenReturn(values);
        when(row.getChecksum()).thenReturn(checksum);
        when(row.shouldSkip()).thenReturn(values == null);
        return row;
    }

    private void givenClientHasNoFailures() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        @SuppressWarnings("unchecked")
        ActionFuture<BulkResponse> responseFuture = mock(ActionFuture.class);
        when(responseFuture.actionGet()).thenReturn(new BulkResponse(new BulkItemResponse[0], 0));
        when(client.execute(same(BulkAction.INSTANCE), bulkRequestCaptor.capture())).thenReturn(responseFuture);
        when(client.threadPool()).thenReturn(threadPool);
    }

    private static class DelegateStubDataExtractor {

        private final List<List<DataFrameDataExtractor.Row>> batches;
        private int batchIndex;

        private DelegateStubDataExtractor(List<List<DataFrameDataExtractor.Row>> batches) {
            this.batches = batches;
        }

        public boolean hasNext() {
            return batchIndex < batches.size();
        }

        public Optional<List<DataFrameDataExtractor.Row>> next() {
            return Optional.of(batches.get(batchIndex++));
        }
    }
}
