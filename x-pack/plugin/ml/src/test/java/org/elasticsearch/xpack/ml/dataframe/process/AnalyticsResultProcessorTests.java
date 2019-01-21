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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.dataframe.DataFrameDataExtractor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnalyticsResultProcessorTests extends ESTestCase {

    private Client client;
    private AnalyticsProcess process;
    private DataFrameDataExtractor dataExtractor;
    private ArgumentCaptor<BulkRequest> bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        process = mock(AnalyticsProcess.class);
        dataExtractor = mock(DataFrameDataExtractor.class);
    }

    public void testProcess_GivenNoResults() {
        givenProcessResults(Collections.emptyList());
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verifyNoMoreInteractions(client);
    }

    public void testProcess_GivenSingleRowAndResult() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row row = newRow(newHit(dataDoc), dataValues, 1);
        givenSingleDataFrameBatch(Arrays.asList(row));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        AnalyticsResult result = new AnalyticsResult(1, resultFields);
        givenProcessResults(Arrays.asList(result));

        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

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

    public void testProcess_GivenSingleRowAndResultWithMismatchingIdHash() throws IOException {
        givenClientHasNoFailures();

        String dataDoc = "{\"f_1\": \"foo\", \"f_2\": 42.0}";
        String[] dataValues = {"42.0"};
        DataFrameDataExtractor.Row row = newRow(newHit(dataDoc), dataValues, 1);
        givenSingleDataFrameBatch(Arrays.asList(row));

        Map<String, Object> resultFields = new HashMap<>();
        resultFields.put("a", "1");
        resultFields.put("b", "2");
        AnalyticsResult result = new AnalyticsResult(2, resultFields);
        givenProcessResults(Arrays.asList(result));

        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verifyNoMoreInteractions(client);
    }

    private void givenProcessResults(List<AnalyticsResult> results) {
        when(process.readAnalyticsResults()).thenReturn(results.iterator());
    }

    private void givenSingleDataFrameBatch(List<DataFrameDataExtractor.Row> batch) throws IOException {
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(Optional.of(batch)).thenReturn(Optional.empty());
    }

    private static SearchHit newHit(String json) {
        SearchHit hit = new SearchHit(randomInt(), randomAlphaOfLength(10), new Text("doc"), Collections.emptyMap());
        hit.sourceRef(new BytesArray(json));
        return hit;
    }

    private static DataFrameDataExtractor.Row newRow(SearchHit hit, String[] values, int checksum) {
        DataFrameDataExtractor.Row row = mock(DataFrameDataExtractor.Row.class);
        when(row.getHit()).thenReturn(hit);
        when(row.getValues()).thenReturn(values);
        when(row.getChecksum()).thenReturn(checksum);
        return row;
    }

    private void givenClientHasNoFailures() {
        ActionFuture<BulkResponse> responseFuture = mock(ActionFuture.class);
        when(responseFuture.actionGet()).thenReturn(new BulkResponse(new BulkItemResponse[0], 0));
        when(client.execute(same(BulkAction.INSTANCE), bulkRequestCaptor.capture())).thenReturn(responseFuture);
    }

    private AnalyticsResultProcessor createResultProcessor() {
        return new AnalyticsResultProcessor(client, dataExtractor);
    }
}
