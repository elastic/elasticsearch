/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetectionTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.ml.dataframe.traintestsplit.TrainTestSplitterFactory;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.SourceField;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameDataExtractorTests extends ESTestCase {

    private static final String JOB_ID = "foo";

    private Client client;
    private List<String> indices;
    private ExtractedFields extractedFields;
    private QueryBuilder query;
    private int scrollSize;
    private Map<String, String> headers;
    private TrainTestSplitterFactory trainTestSplitterFactory;
    private ArgumentCaptor<ClearScrollRequest> capturedClearScrollRequests;
    private ActionFuture<ClearScrollResponse> clearScrollFuture;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        indices = Arrays.asList("index-1", "index-2");
        query = QueryBuilders.matchAllQuery();
        extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("keyword")),
            new DocValueField("field_2", Collections.singleton("keyword"))), Collections.emptyMap());
        scrollSize = 1000;
        headers = Collections.emptyMap();

        trainTestSplitterFactory = mock(TrainTestSplitterFactory.class);
        when(trainTestSplitterFactory.create()).thenReturn(row -> true);

        clearScrollFuture = mock(ActionFuture.class);
        capturedClearScrollRequests = ArgumentCaptor.forClass(ClearScrollRequest.class);
        when(client.execute(same(ClearScrollAction.INSTANCE), capturedClearScrollRequests.capture())).thenReturn(clearScrollFuture);
    }

    public void testTwoPageExtraction() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, false);

        // First batch
        SearchResponse response1 = createSearchResponse(Arrays.asList(1_1, 1_2, 1_3), Arrays.asList(2_1, 2_2, 2_3));
        dataExtractor.setNextResponse(response1);

        // Second batch
        SearchResponse response2 = createSearchResponse(Arrays.asList(3_1), Arrays.asList(4_1));
        dataExtractor.setNextResponse(response2);

        // Third batch is empty
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(3));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(rows.get().get(1).getValues(), equalTo(new String[] {"12", "22"}));
        assertThat(rows.get().get(2).getValues(), equalTo(new String[] {"13", "23"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // Second batch
        rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"31", "41"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // Third batch should return empty
        rows = dataExtractor.next();
        assertThat(rows.isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));

        // Now let's assert we're sending the expected search request
        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(1));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("allowPartialSearchResults=false"));
        assertThat(searchRequest, containsString("indices=[index-1,index-2]"));
        assertThat(searchRequest, containsString("\"size\":1000"));
        assertThat(searchRequest, containsString("\"query\":{\"match_all\":{\"boost\":1.0}}"));
        assertThat(searchRequest, containsString("\"docvalue_fields\":[{\"field\":\"field_1\"},{\"field\":\"field_2\"}]"));
        assertThat(searchRequest, containsString("\"_source\":{\"includes\":[],\"excludes\":[]}"));
        assertThat(searchRequest, containsString("\"sort\":[{\"ml__id_copy\":{\"order\":\"asc\"}}]"));

        // Check continue scroll requests had correct ids
        assertThat(dataExtractor.capturedContinueScrollIds.size(), equalTo(2));
        assertThat(dataExtractor.capturedContinueScrollIds.get(0), equalTo(response1.getScrollId()));
        assertThat(dataExtractor.capturedContinueScrollIds.get(1), equalTo(response2.getScrollId()));

        // Check we cleared the scroll with the latest scroll id
        List<String> capturedClearScrollRequests = getCapturedClearScrollIds();
        assertThat(capturedClearScrollRequests.size(), equalTo(1));
        assertThat(capturedClearScrollRequests.get(0), equalTo(lastAndEmptyResponse.getScrollId()));
    }

    public void testRecoveryFromErrorOnSearchAfterRetry() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, false);

        // First search will fail
        dataExtractor.setNextResponse(createResponseWithShardFailures());

        // Next one will succeed
        SearchResponse response = createSearchResponse(Arrays.asList(1_1), Arrays.asList(2_1));
        dataExtractor.setNextResponse(response);

        // Last one
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch expected as normally since we'll retry after the error
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // Next batch should return empty
        rows = dataExtractor.next();
        assertThat(rows.isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));

        // Check we cleared the scroll with the latest scroll id
        List<String> capturedClearScrollRequests = getCapturedClearScrollIds();
        assertThat(capturedClearScrollRequests.size(), equalTo(1));
        assertThat(capturedClearScrollRequests.get(0), equalTo(lastAndEmptyResponse.getScrollId()));
    }

    public void testErrorOnSearchTwiceLeadsToFailure() {
        TestExtractor dataExtractor = createExtractor(true, false);

        // First search will fail
        dataExtractor.setNextResponse(createResponseWithShardFailures());
        // Next one fails again
        dataExtractor.setNextResponse(createResponseWithShardFailures());

        assertThat(dataExtractor.hasNext(), is(true));

        expectThrows(RuntimeException.class, () -> dataExtractor.next());
    }

    public void testRecoveryFromErrorOnContinueScrollAfterRetry() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, false);

        // Search will succeed
        SearchResponse response1 = createSearchResponse(Arrays.asList(1_1), Arrays.asList(2_1));
        dataExtractor.setNextResponse(response1);

        // But the first continue scroll fails
        dataExtractor.setNextResponse(createResponseWithShardFailures());

        // The next one succeeds and we shall recover
        SearchResponse response2 = createSearchResponse(Arrays.asList(1_2), Arrays.asList(2_2));
        dataExtractor.setNextResponse(response2);

        // Last one
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch expected as normally since we'll retry after the error
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // We get second batch as we retried after the error
        rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"12", "22"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // Next batch should return empty
        rows = dataExtractor.next();
        assertThat(rows.isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));

        // Notice we've done two searches and two continues here
        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(2));
        assertThat(dataExtractor.capturedContinueScrollIds.size(), equalTo(2));

        // Check we cleared the scroll with the latest scroll id
        List<String> capturedClearScrollRequests = getCapturedClearScrollIds();
        assertThat(capturedClearScrollRequests.size(), equalTo(1));
        assertThat(capturedClearScrollRequests.get(0), equalTo(lastAndEmptyResponse.getScrollId()));
    }

    public void testErrorOnContinueScrollTwiceLeadsToFailure() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, false);

        // Search will succeed
        SearchResponse response1 = createSearchResponse(Arrays.asList(1_1), Arrays.asList(2_1));
        dataExtractor.setNextResponse(response1);

        // But the first continue scroll fails
        dataExtractor.setNextResponse(createResponseWithShardFailures());
        // As well as the second
        dataExtractor.setNextResponse(createResponseWithShardFailures());

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch expected as normally since we'll retry after the error
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // We get second batch as we retried after the error
        expectThrows(RuntimeException.class, () -> dataExtractor.next());
    }

    public void testIncludeSourceIsFalseAndNoSourceFields() throws IOException {
        TestExtractor dataExtractor = createExtractor(false, false);

        SearchResponse response = createSearchResponse(Arrays.asList(1_1), Arrays.asList(2_1));
        dataExtractor.setNextResponse(response);
        dataExtractor.setNextResponse(createEmptySearchResponse());

        assertThat(dataExtractor.hasNext(), is(true));

        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(dataExtractor.hasNext(), is(true));

        assertThat(dataExtractor.next().isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));

        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(1));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"docvalue_fields\":[{\"field\":\"field_1\"},{\"field\":\"field_2\"}]"));
        assertThat(searchRequest, containsString("\"_source\":false"));
    }

    public void testIncludeSourceIsFalseAndAtLeastOneSourceField() throws IOException {
        // Explicit cast of ExtractedField args necessary for Eclipse due to https://bugs.eclipse.org/bugs/show_bug.cgi?id=530915
        extractedFields = new ExtractedFields(Arrays.asList(
            (ExtractedField) new DocValueField("field_1", Collections.singleton("keyword")),
            (ExtractedField) new SourceField("field_2", Collections.singleton("text"))), Collections.emptyMap());

        TestExtractor dataExtractor = createExtractor(false, false);

        SearchResponse response = createSearchResponse(Arrays.asList(1_1), Arrays.asList(2_1));
        dataExtractor.setNextResponse(response);
        dataExtractor.setNextResponse(createEmptySearchResponse());

        assertThat(dataExtractor.hasNext(), is(true));

        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(dataExtractor.hasNext(), is(true));

        assertThat(dataExtractor.next().isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));

        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(1));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"docvalue_fields\":[{\"field\":\"field_1\"}]"));
        assertThat(searchRequest, containsString("\"_source\":{\"includes\":[\"field_2\"],\"excludes\":[]}"));
    }

    public void testCollectDataSummary_GivenAnalysisSupportsMissingFields() {
        TestExtractor dataExtractor = createExtractor(true, true);

        // First and only batch
        SearchResponse response = createSearchResponse(Arrays.asList(1_1, 1_2), Arrays.asList(2_1, 2_2));
        dataExtractor.setNextResponse(response);

        DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();

        assertThat(dataSummary.rows, equalTo(2L));
        assertThat(dataSummary.cols, equalTo(2));

        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(1));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"match_all\":{\"boost\":1.0}}"));
    }

    public void testCollectDataSummary_GivenAnalysisDoesNotSupportMissingFields() {
        TestExtractor dataExtractor = createExtractor(true, false);

        // First and only batch
        SearchResponse response = createSearchResponse(Arrays.asList(1_1, 1_2), Arrays.asList(2_1, 2_2));
        dataExtractor.setNextResponse(response);

        DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();

        assertThat(dataSummary.rows, equalTo(2L));
        assertThat(dataSummary.cols, equalTo(2));

        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(1));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString(
            "\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},{\"bool\":{\"filter\":" +
                "[{\"exists\":{\"field\":\"field_1\",\"boost\":1.0}},{\"exists\":{\"field\":\"field_2\",\"boost\":1.0}}]," +
                "\"boost\":1.0}}],\"boost\":1.0}"));
    }

    public void testMissingValues_GivenSupported() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, true);

        // First and only batch
        SearchResponse response1 = createSearchResponse(Arrays.asList(1_1, null, 1_3), Arrays.asList(2_1, 2_2, 2_3));
        dataExtractor.setNextResponse(response1);

        // Empty
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(3));

        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(rows.get().get(1).getValues()[0], equalTo(DataFrameDataExtractor.NULL_VALUE));
        assertThat(rows.get().get(1).getValues()[1], equalTo("22"));
        assertThat(rows.get().get(2).getValues(), equalTo(new String[] {"13", "23"}));

        assertThat(rows.get().get(0).shouldSkip(), is(false));
        assertThat(rows.get().get(1).shouldSkip(), is(false));
        assertThat(rows.get().get(2).shouldSkip(), is(false));

        assertThat(dataExtractor.hasNext(), is(true));

        // Third batch should return empty
        rows = dataExtractor.next();
        assertThat(rows.isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));
    }

    public void testMissingValues_GivenNotSupported() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, false);

        // First and only batch
        SearchResponse response1 = createSearchResponse(Arrays.asList(1_1, null, 1_3), Arrays.asList(2_1, 2_2, 2_3));
        dataExtractor.setNextResponse(response1);

        // Empty
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(3));

        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(rows.get().get(1).getValues(), is(nullValue()));
        assertThat(rows.get().get(2).getValues(), equalTo(new String[] {"13", "23"}));

        assertThat(rows.get().get(0).shouldSkip(), is(false));
        assertThat(rows.get().get(1).shouldSkip(), is(true));
        assertThat(rows.get().get(2).shouldSkip(), is(false));

        assertThat(dataExtractor.hasNext(), is(true));

        // Third batch should return empty
        rows = dataExtractor.next();
        assertThat(rows.isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));
    }

    public void testGetCategoricalFields() {
        // Explicit cast of ExtractedField args necessary for Eclipse due to https://bugs.eclipse.org/bugs/show_bug.cgi?id=530915
        extractedFields = new ExtractedFields(Arrays.asList(
            (ExtractedField) new DocValueField("field_boolean", Collections.singleton("boolean")),
            (ExtractedField) new DocValueField("field_float", Collections.singleton("float")),
            (ExtractedField) new DocValueField("field_double", Collections.singleton("double")),
            (ExtractedField) new DocValueField("field_byte", Collections.singleton("byte")),
            (ExtractedField) new DocValueField("field_short", Collections.singleton("short")),
            (ExtractedField) new DocValueField("field_integer", Collections.singleton("integer")),
            (ExtractedField) new DocValueField("field_long", Collections.singleton("long")),
            (ExtractedField) new DocValueField("field_keyword", Collections.singleton("keyword")),
            (ExtractedField) new SourceField("field_text", Collections.singleton("text"))), Collections.emptyMap());
        TestExtractor dataExtractor = createExtractor(true, true);

        assertThat(dataExtractor.getCategoricalFields(OutlierDetectionTests.createRandom()), empty());

        assertThat(dataExtractor.getCategoricalFields(new Regression("field_double")), containsInAnyOrder("field_keyword", "field_text"));
        assertThat(dataExtractor.getCategoricalFields(new Regression("field_long")), containsInAnyOrder("field_keyword", "field_text"));
        assertThat(dataExtractor.getCategoricalFields(new Regression("field_boolean")), containsInAnyOrder("field_keyword", "field_text"));

        assertThat(
            dataExtractor.getCategoricalFields(new Classification("field_keyword")),
            containsInAnyOrder("field_keyword", "field_text"));
        assertThat(
            dataExtractor.getCategoricalFields(new Classification("field_long")),
            containsInAnyOrder("field_keyword", "field_text", "field_long"));
        assertThat(
            dataExtractor.getCategoricalFields(new Classification("field_boolean")),
            containsInAnyOrder("field_keyword", "field_text", "field_boolean"));
    }

    private TestExtractor createExtractor(boolean includeSource, boolean supportsRowsWithMissingValues) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(JOB_ID, extractedFields, indices, query, scrollSize,
            headers, includeSource, supportsRowsWithMissingValues, trainTestSplitterFactory);
        return new TestExtractor(client, context);
    }

    private SearchResponse createSearchResponse(List<Number> field1Values, List<Number> field2Values) {
        assertThat(field1Values.size(), equalTo(field2Values.size()));
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < field1Values.size(); i++) {
            SearchHitBuilder searchHitBuilder = new SearchHitBuilder(randomInt());
            addField(searchHitBuilder, "field_1", field1Values.get(i));
            addField(searchHitBuilder, "field_2", field2Values.get(i));
            searchHitBuilder.setSource("{\"field_1\":" + field1Values.get(i) + ",\"field_2\":" + field2Values.get(i) + "}");
            hits.add(searchHitBuilder.build());
        }
        SearchHits searchHits = new SearchHits(hits.toArray(new SearchHit[0]), new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1);
        when(searchResponse.getHits()).thenReturn(searchHits);
        return searchResponse;
    }

    private static void addField(SearchHitBuilder searchHitBuilder, String field, @Nullable Number value) {
        searchHitBuilder.addField(field, value == null ? Collections.emptyList() : Collections.singletonList(value));
    }

    private SearchResponse createEmptySearchResponse() {
        return createSearchResponse(Collections.emptyList(), Collections.emptyList());
    }

    private SearchResponse createResponseWithShardFailures() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getShardFailures()).thenReturn(
            new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("shard failed"))});
        when(searchResponse.getFailedShards()).thenReturn(1);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        return searchResponse;
    }

    private List<String> getCapturedClearScrollIds() {
        return capturedClearScrollRequests.getAllValues().stream().map(r -> r.getScrollIds().get(0)).collect(Collectors.toList());
    }

    private static class TestExtractor extends DataFrameDataExtractor {

        private Queue<SearchResponse> responses = new LinkedList<>();
        private List<SearchRequestBuilder> capturedSearchRequests = new ArrayList<>();
        private List<String> capturedContinueScrollIds = new ArrayList<>();

        TestExtractor(Client client, DataFrameDataExtractorContext context) {
            super(client, context);
        }

        void setNextResponse(SearchResponse searchResponse) {
            responses.add(searchResponse);
        }

        @Override
        protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
            capturedSearchRequests.add(searchRequestBuilder);
            SearchResponse searchResponse = responses.remove();
            if (searchResponse.getShardFailures() != null) {
                throw new RuntimeException(searchResponse.getShardFailures()[0].getCause());
            }
            return searchResponse;
        }

        @Override
        protected SearchResponse executeSearchScrollRequest(String scrollId) {
            capturedContinueScrollIds.add(scrollId);
            SearchResponse searchResponse = responses.remove();
            if (searchResponse.getShardFailures() != null) {
                throw new RuntimeException(searchResponse.getShardFailures()[0].getCause());
            }
            return searchResponse;
        }
    }
}
