/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
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
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.ml.dataframe.traintestsplit.TrainTestSplitterFactory;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.ProcessedField;
import org.elasticsearch.xpack.ml.extractor.SourceField;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
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
    private long searchHitCounter;

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
            new DocValueField("field_2", Collections.singleton("keyword"))),
            Collections.emptyList(),
            Collections.emptyMap());
        scrollSize = 1000;
        headers = Collections.emptyMap();

        trainTestSplitterFactory = mock(TrainTestSplitterFactory.class);
        when(trainTestSplitterFactory.create()).thenReturn(row -> true);
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

        // Now let's assert we're sending the expected search requests
        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(3));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("allowPartialSearchResults=false"));
        assertThat(searchRequest, containsString("indices=[index-1,index-2]"));
        assertThat(searchRequest, containsString("\"size\":1000"));
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},{\"range\":" +
            "{\"ml__incremental_id\":{\"from\":0,\"to\":1000,\"include_lower\":true,\"include_upper\":false,\"boost\":1.0}}}]"));
        assertThat(searchRequest, containsString("\"docvalue_fields\":[{\"field\":\"field_1\"},{\"field\":\"field_2\"}]"));
        assertThat(searchRequest, containsString("\"_source\":{\"includes\":[],\"excludes\":[]}"));
        assertThat(searchRequest, containsString("\"sort\":[{\"ml__incremental_id\":{\"order\":\"asc\"}}]"));

        searchRequest = dataExtractor.capturedSearchRequests.get(1).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},{\"range\":" +
            "{\"ml__incremental_id\":{\"from\":3,\"to\":1003,\"include_lower\":true,\"include_upper\":false,\"boost\":1.0}}}]"));

        searchRequest = dataExtractor.capturedSearchRequests.get(2).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},{\"range\":" +
            "{\"ml__incremental_id\":{\"from\":4,\"to\":1004,\"include_lower\":true,\"include_upper\":false,\"boost\":1.0}}}]"));
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

    public void testRecoveryFromErrorOnSearch() throws IOException {
        TestExtractor dataExtractor = createExtractor(true, false);

        // First search will succeed
        SearchResponse response1 = createSearchResponse(Arrays.asList(1_1, 1_2), Arrays.asList(2_1, 2_2));
        dataExtractor.setNextResponse(response1);

        // But the second search fails
        dataExtractor.setNextResponse(createResponseWithShardFailures());

        // The next one succeeds and we shall recover
        SearchResponse response2 = createSearchResponse(Arrays.asList(1_3), Arrays.asList(2_3));
        dataExtractor.setNextResponse(response2);

        // Last one
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch expected as normally since we'll retry after the error
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(2));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"11", "21"}));
        assertThat(rows.get().get(1).getValues(), equalTo(new String[] {"12", "22"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // We get second batch as we retried after the error
        rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(1));
        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"13", "23"}));
        assertThat(dataExtractor.hasNext(), is(true));

        // Next batch should return empty
        rows = dataExtractor.next();
        assertThat(rows.isEmpty(), is(true));
        assertThat(dataExtractor.hasNext(), is(false));

        // Notice we've done 4 searches
        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(4));

        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{"));
        assertThat(searchRequest, containsString("{\"match_all\":{\"boost\":1.0}"));
        assertThat(searchRequest, containsString(
            "{\"range\":{\"ml__incremental_id\":{\"from\":0,\"to\":1000,\"include_lower\":true,\"include_upper\":false"));

        // Assert the second search continued from the latest successfully processed doc
        searchRequest = dataExtractor.capturedSearchRequests.get(1).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{"));
        assertThat(searchRequest, containsString("{\"match_all\":{\"boost\":1.0}"));
        assertThat(searchRequest, containsString(
            "{\"range\":{\"ml__incremental_id\":{\"from\":2,\"to\":1002,\"include_lower\":true,\"include_upper\":false"));

        // Assert the third search continued from the latest successfully processed doc
        searchRequest = dataExtractor.capturedSearchRequests.get(2).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{"));
        assertThat(searchRequest, containsString("{\"match_all\":{\"boost\":1.0}"));
        assertThat(searchRequest, containsString(
            "{\"range\":{\"ml__incremental_id\":{\"from\":2,\"to\":1002,\"include_lower\":true,\"include_upper\":false"));

        searchRequest = dataExtractor.capturedSearchRequests.get(3).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{"));
        assertThat(searchRequest, containsString("{\"match_all\":{\"boost\":1.0}"));
        assertThat(searchRequest, containsString(
            "{\"range\":{\"ml__incremental_id\":{\"from\":3,\"to\":1003,\"include_lower\":true,\"include_upper\":false"));
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

        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(2));
        String searchRequest = dataExtractor.capturedSearchRequests.get(0).request().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"docvalue_fields\":[{\"field\":\"field_1\"},{\"field\":\"field_2\"}]"));
        assertThat(searchRequest, containsString("\"_source\":false"));
    }

    public void testIncludeSourceIsFalseAndAtLeastOneSourceField() throws IOException {
        // Explicit cast of ExtractedField args necessary for Eclipse due to https://bugs.eclipse.org/bugs/show_bug.cgi?id=530915
        extractedFields = new ExtractedFields(Arrays.asList(
            (ExtractedField) new DocValueField("field_1", Collections.singleton("keyword")),
            (ExtractedField) new SourceField("field_2", Collections.singleton("text"))),
            Collections.emptyList(),
            Collections.emptyMap());

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

        assertThat(dataExtractor.capturedSearchRequests.size(), equalTo(2));
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
            (ExtractedField) new SourceField("field_text", Collections.singleton("text"))),
            Collections.emptyList(),
            Collections.emptyMap());
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

    public void testGetFieldNames_GivenProcessesFeatures() {
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
            (ExtractedField) new SourceField("field_text", Collections.singleton("text"))),
            Arrays.asList(
                new ProcessedField(new CategoricalPreProcessor("field_long", "animal")),
                buildProcessedField("field_short", "field_1", "field_2")
            ),
            Collections.emptyMap());
        TestExtractor dataExtractor = createExtractor(true, true);

        assertThat(dataExtractor.getCategoricalFields(new Regression("field_double")),
            containsInAnyOrder("field_keyword", "field_text", "animal"));

        List<String> fieldNames = dataExtractor.getFieldNames();
        assertThat(fieldNames, containsInAnyOrder(
            "animal",
            "field_1",
            "field_2",
            "field_boolean",
            "field_float",
            "field_double",
            "field_byte",
            "field_integer",
            "field_keyword",
            "field_text"));
        assertThat(dataExtractor.getFieldNames(), contains(fieldNames.toArray(String[]::new)));
    }

    public void testExtractionWithProcessedFeatures() throws IOException {
        extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("keyword")),
            new DocValueField("field_2", Collections.singleton("keyword"))),
            Arrays.asList(
                new ProcessedField(new CategoricalPreProcessor("field_1", "animal")),
                new ProcessedField(new OneHotEncoding("field_1",
                    Arrays.asList("11", "12")
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), s -> s.equals("11") ? "field_11" : "field_12")),
                    true))
            ),
            Collections.emptyMap());

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

        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"21", "dog", "1", "0"}));
        assertThat(rows.get().get(1).getValues(),
            equalTo(new String[] {"22", "dog", DataFrameDataExtractor.NULL_VALUE, DataFrameDataExtractor.NULL_VALUE}));
        assertThat(rows.get().get(2).getValues(), equalTo(new String[] {"23", "dog", "0", "0"}));

        assertThat(rows.get().get(0).shouldSkip(), is(false));
        assertThat(rows.get().get(1).shouldSkip(), is(false));
        assertThat(rows.get().get(2).shouldSkip(), is(false));
    }

    public void testExtractionWithMultipleScalarTypesInSource() throws IOException {
        extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("keyword")),
            new DocValueField("field_2", Collections.singleton("keyword"))),
            Collections.emptyList(),
            Collections.emptyMap());

        TestExtractor dataExtractor = createExtractor(true, true);

        // First and only batch
        SearchResponse response1 = createSearchResponse(Arrays.asList(1, "true", false), Arrays.asList(2_1, 2_2, 2_3));
        dataExtractor.setNextResponse(response1);

        // Empty
        SearchResponse lastAndEmptyResponse = createEmptySearchResponse();
        dataExtractor.setNextResponse(lastAndEmptyResponse);

        assertThat(dataExtractor.hasNext(), is(true));

        // First batch
        Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
        assertThat(rows.isPresent(), is(true));
        assertThat(rows.get().size(), equalTo(3));

        assertThat(rows.get().get(0).getValues(), equalTo(new String[] {"1", "21",}));
        assertThat(rows.get().get(1).getValues(), equalTo(new String[] {"true", "22"}));
        assertThat(rows.get().get(2).getValues(), equalTo(new String[] {"false", "23"}));

        assertThat(rows.get().get(0).shouldSkip(), is(false));
        assertThat(rows.get().get(1).shouldSkip(), is(false));
        assertThat(rows.get().get(2).shouldSkip(), is(false));
    }

    public void testExtractionWithProcessedFieldThrows() {
        ProcessedField processedField = mock(ProcessedField.class);
        doThrow(new RuntimeException("process field error")).when(processedField).value(any(), any());

        extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("keyword")),
            new DocValueField("field_2", Collections.singleton("keyword"))),
            Collections.singletonList(processedField),
            Collections.emptyMap());

        TestExtractor dataExtractor = createExtractor(true, true);

        SearchResponse response = createSearchResponse(Arrays.asList(1_1, null, 1_3), Arrays.asList(2_1, 2_2, 2_3));
        dataExtractor.setAlwaysResponse(response);

        assertThat(dataExtractor.hasNext(), is(true));

        expectThrows(RuntimeException.class, () -> dataExtractor.next());
    }

    private TestExtractor createExtractor(boolean includeSource, boolean supportsRowsWithMissingValues) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(JOB_ID, extractedFields, indices, query, scrollSize,
            headers, includeSource, supportsRowsWithMissingValues, trainTestSplitterFactory, Collections.emptyMap());
        return new TestExtractor(client, context);
    }

    private static ProcessedField buildProcessedField(String inputField, String... outputFields) {
        return new ProcessedField(buildPreProcessor(inputField, outputFields));
    }

    private static PreProcessor buildPreProcessor(String inputField, String... outputFields) {
        return new OneHotEncoding(inputField,
            Arrays.stream(outputFields).collect(Collectors.toMap((s) -> randomAlphaOfLength(10), Function.identity())),
            true);
    }

    private SearchResponse createSearchResponse(List<Object> field1Values, List<Object> field2Values) {
        assertThat(field1Values.size(), equalTo(field2Values.size()));
        SearchResponse searchResponse = mock(SearchResponse.class);
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < field1Values.size(); i++) {
            SearchHitBuilder searchHitBuilder = new SearchHitBuilder(randomInt());
            addField(searchHitBuilder, "field_1", field1Values.get(i));
            addField(searchHitBuilder, "field_2", field2Values.get(i));
            searchHitBuilder.setSource("{\"field_1\":" + field1Values.get(i) + ",\"field_2\":" + field2Values.get(i) + "}");
            searchHitBuilder.setLongSortValue(searchHitCounter++);
            hits.add(searchHitBuilder.build());
        }
        SearchHits searchHits = new SearchHits(hits.toArray(new SearchHit[0]), new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1);
        when(searchResponse.getHits()).thenReturn(searchHits);
        return searchResponse;
    }

    private static void addField(SearchHitBuilder searchHitBuilder, String field, @Nullable Object value) {
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

    private static class TestExtractor extends DataFrameDataExtractor {

        private Queue<SearchResponse> responses = new LinkedList<>();
        private List<SearchRequestBuilder> capturedSearchRequests = new ArrayList<>();
        private SearchResponse alwaysResponse;

        TestExtractor(Client client, DataFrameDataExtractorContext context) {
            super(client, context);
        }

        void setNextResponse(SearchResponse searchResponse) {
            if (alwaysResponse != null) {
                throw new IllegalStateException("Should not set next response when an always response has been set");
            }
            responses.add(searchResponse);
        }

        void setAlwaysResponse(SearchResponse searchResponse) {
            alwaysResponse = searchResponse;
        }

        @Override
        protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
            capturedSearchRequests.add(searchRequestBuilder);
            SearchResponse searchResponse =  alwaysResponse == null ? responses.remove() : alwaysResponse;
            if (searchResponse.getShardFailures() != null) {
                throw new RuntimeException(searchResponse.getShardFailures()[0].getCause());
            }
            return searchResponse;
        }
    }

    private static class CategoricalPreProcessor implements PreProcessor {

        private final List<String> inputFields;
        private final List<String> outputFields;

        CategoricalPreProcessor(String inputField, String outputField) {
            this.inputFields = Arrays.asList(inputField);
            this.outputFields = Arrays.asList(outputField);
        }

        @Override
        public List<String> inputFields() {
            return inputFields;
        }

        @Override
        public List<String> outputFields() {
            return outputFields;
        }

        @Override
        public void process(Map<String, Object> fields) {
            fields.put(outputFields.get(0), "dog");
        }

        @Override
        public Map<String, String> reverseLookup() {
            return null;
        }

        @Override
        public boolean isCustom() {
            return true;
        }

        @Override
        public String getOutputFieldType(String outputField) {
            return "text";
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }
}
