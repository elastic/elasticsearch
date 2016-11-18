/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

public class ElasticsearchDataExtractorTests extends ESTestCase {

    private static final String BASE_URL = "http://localhost:9200";
    private static final List<String> INDEXES = Arrays.asList("index-*");
    private static final List<String> TYPES = Arrays.asList("dataType");
    private static final String SEARCH = "\"match_all\":{}";
    private static final String TIME_FIELD = "time";
    private static final String CLEAR_SCROLL_RESPONSE = "{}";

    private Logger jobLogger;

    private String aggregations;
    private String scriptFields;
    private String fields;

    private ElasticsearchDataExtractor extractor;

    @Before
    public void setUpTests() throws IOException {
        jobLogger = mock(Logger.class);
    }

    public void testDataExtraction() throws IOException {
        String initialResponse = "{" + "\"_scroll_id\":\"c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"," + "\"took\":17,"
                + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "},"
                + "\"hits\":{" + "  \"total\":1437," + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\","
                + "    \"_type\":\"dataType\"," + "    \"_id\":\"1403481600\"," + "    \"_score\":null," + "    \"_source\":{"
                + "      \"id\":\"1403481600\"" + "    }" + "  ]" + "}" + "}";

        String scrollResponse = "{" + "\"_scroll_id\":\"secondScrollId\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403782200\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403782200\"" + "    }"
                + "  ]" + "}" + "}";

        String scrollEndResponse = "{" + "\"_scroll_id\":\"thirdScrollId\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[]" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200),
                new HttpResponse(toStream(scrollResponse), 200), new HttpResponse(toStream(scrollEndResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertEquals(initialResponse, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertEquals(scrollResponse, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());

        requester.assertEqualRequestsToResponses();
        requester.assertResponsesHaveBeenConsumed();

        RequestParams firstRequestParams = requester.getGetRequestParams(0);
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", firstRequestParams.url);
        String expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {"
                + "    \"bool\": {" + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"time\": {" + "              \"gte\": \"1970-01-17T04:53:20.000Z\","
                + "              \"lt\": \"1970-01-17T05:53:20.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }" + "}";
        assertEquals(expectedSearchBody.replaceAll(" ", ""), firstRequestParams.requestBody.replaceAll(" ", ""));

        RequestParams secondRequestParams = requester.getGetRequestParams(1);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", secondRequestParams.url);
        assertEquals("c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1", secondRequestParams.requestBody);

        RequestParams thirdRequestParams = requester.getGetRequestParams(2);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", thirdRequestParams.url);
        assertEquals("secondScrollId", thirdRequestParams.requestBody);

        assertEquals("http://localhost:9200/_search/scroll", requester.getDeleteRequestParams(0).url);
        assertEquals("{\"scroll_id\":[\"thirdScrollId\"]}", requester.getDeleteRequestParams(0).requestBody);
        assertEquals(1, requester.deleteRequestParams.size());
    }

    public void testDataExtraction_GivenInitialResponseContainsLongScrollId() throws IOException {
        StringBuilder scrollId = new StringBuilder();
        for (int i = 0; i < 300 * 1024; i++) {
            scrollId.append("a");
        }

        String initialResponse = "{" + "\"_scroll_id\":\"" + scrollId + "\"," + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403481600\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }"
                + "  ]" + "}" + "}";

        String scrollEndResponse = "{" + "\"_scroll_id\":\"" + scrollId + "\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[]" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200),
                new HttpResponse(toStream(scrollEndResponse), 200));
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        extractor.next();
        assertTrue(extractor.hasNext());
        extractor.next();
        assertFalse(extractor.hasNext());

        assertEquals(scrollId.toString(), requester.getGetRequestParams(1).requestBody);
    }

    public void testDataExtraction_GivenInitialResponseContainsNoHitsAndNoScrollId() throws IOException {
        String initialResponse = "{}";
        HttpResponse httpGetResponse = new HttpResponse(toStream(initialResponse), 200);
        List<HttpResponse> responses = Arrays.asList(httpGetResponse);
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        IOException e = expectThrows(IOException.class, () -> extractor.next());
        assertEquals("Field '_scroll_id' was expected but not found in first 2 bytes of response:\n{}", e.getMessage());
    }

    public void testDataExtraction_GivenInitialResponseContainsHitsButNoScrollId() throws IOException {
        String initialResponse = "{" + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1," + "  \"successful\":1,"
                + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437," + "  \"max_score\":null," + "  \"hits\":["
                + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\"," + "    \"_id\":\"1403481600\"," + "    \"_score\":null,"
                + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }" + "  ]" + "}" + "}";

        HttpResponse httpGetResponse = new HttpResponse(toStream(initialResponse), 200);
        List<HttpResponse> responses = Arrays.asList(httpGetResponse);
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        IOException e = expectThrows(IOException.class, () -> extractor.next());
        assertEquals("Field '_scroll_id' was expected but not found in first 272 bytes of response:\n" + initialResponse, e.getMessage());
    }

    public void testDataExtraction_GivenInitialResponseContainsTooLongScrollId() throws IOException {
        StringBuilder scrollId = new StringBuilder();
        for (int i = 0; i < 1024 * 1024; i++) {
            scrollId.append("a");
        }

        String initialResponse = "{" + "\"_scroll_id\":\"" + scrollId + "\"," + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403481600\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }"
                + "  ]" + "}" + "}";

        HttpResponse httpGetResponse = new HttpResponse(toStream(initialResponse), 200);
        List<HttpResponse> responses = Arrays.asList(httpGetResponse);
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        IOException e = expectThrows(IOException.class, () -> extractor.next());
        assertEquals("Field '_scroll_id' was expected but not found in first 1048576 bytes of response:\n" + initialResponse,
                e.getMessage());
    }

    public void testDataExtraction_GivenInitialResponseDoesNotReturnOk() throws IOException {
        String initialResponse = "{}";
        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 500));
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        IOException e = expectThrows(IOException.class, () -> extractor.next());
        assertEquals(
                "Request 'http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000' failed with status code: 500."
                        + " Response was:\n{}",
                        e.getMessage());
    }

    public void testDataExtraction_GivenScrollResponseDoesNotReturnOk() throws IOException {
        String initialResponse = "{" + "\"_scroll_id\":\"c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"," + "\"hits\":[..." + "}";
        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200),
                new HttpResponse(toStream("{}"), 500));
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertEquals(initialResponse, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        IOException e = expectThrows(IOException.class, () -> extractor.next());
        assertEquals("Request 'http://localhost:9200/_search/scroll?scroll=60m' with scroll id 'c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1' "
                + "failed with status code: 500. Response was:\n{}", e.getMessage());
    }

    public void testNext_ThrowsGivenHasNotNext() throws IOException {
        String initialResponse = "{" + "\"_scroll_id\":\"c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"," + "\"hits\":[]" + "}";
        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200));
        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());
        expectThrows(NoSuchElementException.class, () -> extractor.next());
    }

    public void testDataExtractionWithFields() throws IOException {
        fields = "[\"id\"]";

        String initialResponse = "{" + "\"_scroll_id\":\"c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"," + "\"took\":17,"
                + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "},"
                + "\"hits\":{" + "  \"total\":1437," + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\","
                + "    \"_type\":\"dataType\"," + "    \"_id\":\"1403481600\"," + "    \"_score\":null," + "    \"fields\":{"
                + "      \"id\":[\"1403481600\"]" + "    }" + "  ]" + "}" + "}";

        String scrollResponse = "{" + "\"_scroll_id\":\"secondScrollId\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403782200\"," + "    \"_score\":null," + "    \"fields\":{" + "      \"id\":[\"1403782200\"]" + "    }"
                + "  ]" + "}" + "}";

        String scrollEndResponse = "{" + "\"_scroll_id\":\"thirdScrollId\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1437,"
                + "  \"max_score\":null," + "  \"hits\":[]" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200),
                new HttpResponse(toStream(scrollResponse), 200), new HttpResponse(toStream(scrollEndResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertEquals(initialResponse, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertEquals(scrollResponse, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());

        requester.assertEqualRequestsToResponses();
        requester.assertResponsesHaveBeenConsumed();

        RequestParams firstRequestParams = requester.getGetRequestParams(0);
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", firstRequestParams.url);
        String expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {"
                + "    \"bool\": {" + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"time\": {" + "              \"gte\": \"1970-01-17T04:53:20.000Z\","
                + "              \"lt\": \"1970-01-17T05:53:20.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"_source\": [\"id\"]" + "}";
        assertEquals(expectedSearchBody.replaceAll(" ", ""), firstRequestParams.requestBody.replaceAll(" ", ""));

        RequestParams secondRequestParams = requester.getGetRequestParams(1);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", secondRequestParams.url);
        assertEquals("c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1", secondRequestParams.requestBody);

        RequestParams thirdRequestParams = requester.getGetRequestParams(2);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", thirdRequestParams.url);
        assertEquals("secondScrollId", thirdRequestParams.requestBody);

        assertEquals("http://localhost:9200/_search/scroll", requester.getDeleteRequestParams(0).url);
        assertEquals("{\"scroll_id\":[\"thirdScrollId\"]}", requester.getDeleteRequestParams(0).requestBody);
        assertEquals(1, requester.deleteRequestParams.size());
    }

    public void testDataExtractionWithAggregations() throws IOException {
        aggregations = "{\"my-aggs\": {\"terms\":{\"field\":\"foo\"}}}";

        String initialResponse = "{" + "\"_scroll_id\":\"r2d2bjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"," + "\"took\":17,"
                + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "},"
                + "\"aggregations\":{" + "  \"my-aggs\":{" + "    \"buckets\":[" + "      {" + "        \"key\":\"foo\"" + "      }"
                + "    ]" + "  }" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertEquals(initialResponse, streamToString(extractor.next().get()));
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());

        requester.assertEqualRequestsToResponses();
        requester.assertResponsesHaveBeenConsumed();

        assertEquals(1, requester.getRequestParams.size());
        RequestParams requestParams = requester.getGetRequestParams(0);
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=0", requestParams.url);
        String expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {"
                + "    \"bool\": {" + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"time\": {" + "              \"gte\": \"1970-01-17T04:53:20.000Z\","
                + "              \"lt\": \"1970-01-17T05:53:20.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }," + "  \"aggs\":{\"my-aggs\": {\"terms\":{\"field\":\"foo\"}}}"
                + "}";
        assertEquals(expectedSearchBody.replaceAll(" ", ""), requestParams.requestBody.replaceAll(" ", ""));

        assertEquals("http://localhost:9200/_search/scroll", requester.getDeleteRequestParams(0).url);
        assertEquals("{\"scroll_id\":[\"r2d2bjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"]}", requester.getDeleteRequestParams(0).requestBody);
        assertEquals(1, requester.deleteRequestParams.size());
    }

    public void testDataExtractionWithAggregations_GivenResponseHasEmptyBuckets() throws IOException {
        aggregations = "{\"aggs\":{\"my-aggs\": {\"terms\":{\"field\":\"foo\"}}}}";

        String initialResponse = "{" + "\"_scroll_id\":\"r2d2bjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1\"," + "\"took\":17,"
                + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "},"
                + "\"aggregations\":{" + "  \"my-aggs\":{" + "    \"buckets\":[]" + "  }" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(initialResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000L, 1403600000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());

        requester.assertEqualRequestsToResponses();
        requester.assertResponsesHaveBeenConsumed();

        assertEquals(1, requester.getRequestParams.size());
        RequestParams requestParams = requester.getGetRequestParams(0);
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=0", requestParams.url);
    }

    public void testChunkedDataExtraction() throws IOException {
        String dataSummaryResponse = "{" + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1,"
                + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000," + "  \"max_score\":null,"
                + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\"," + "    \"_id\":\"1403481600\","
                + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }" + "  ]" + "},"
                + "\"aggregations\":{" + "\"earliestTime\":{" + "\"value\":1400000001000," + "\"value_as_string\":\"2014-05-13T16:53:21Z\""
                + "}," + "\"latestTime\":{" + "\"value\":1400007201000," + "\"value_as_string\":\"2014-05-13T17:16:01Z\"" + "}" + "}" + "}";

        String indexResponse = "{" + "\"dataIndex\":{" + "  \"settings\":{" + "    \"index\":{" + "      \"creation_date\":0,"
                + "      \"number_of_shards\":\"5\"," + "      \"number_of_replicas\":\"1\"" + "    }" + "  }" + "}";

        String initialResponse1 = "{" + "\"_scroll_id\":\"scrollId_1\"," + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403481600\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }"
                + "  ]" + "}" + "}";

        String continueResponse1 = "{" + "\"_scroll_id\":\"scrollId_2\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403782200\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403782200\"" + "    }"
                + "  ]" + "}" + "}";

        String endResponse1 = "{" + "\"_scroll_id\":\"scrollId_3\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000,"
                + "  \"max_score\":null," + "  \"hits\":[]" + "}" + "}";

        String initialResponse2 = "{" + "\"_scroll_id\":\"scrollId_4\"," + "\"hits\":{" + "  \"total\":10000," + "  \"hits\":["
                + "    \"_index\":\"dataIndex\"" + "  ]" + "}" + "}";

        String endResponse2 = "{" + "\"_scroll_id\":\"scrollId_5\"," + "\"hits\":[]" + "}";

        String initialResponse3 = "{" + "\"_scroll_id\":\"scrollId_6\"," + "\"hits\":[]" + "}";

        String dataSummaryResponse2 = "{" + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1,"
                + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":1," + "  \"max_score\":null,"
                + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\"," + "    \"_id\":\"1403481600\","
                + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }" + "  ]" + "},"
                + "\"aggregations\":{" + "\"earliestTime\":{" + "\"value\":1400007201000," + "\"value_as_string\":\"2014-05-13T17:16:01Z\""
                + "}," + "\"latestTime\":{" + "\"value\":1400007201000," + "\"value_as_string\":\"2014-05-13T17:16:01Z\"" + "}" + "}" + "}";

        String initialResponse4 = "{" + "\"_scroll_id\":\"scrollId_7\"," + "\"hits\":{" + "  \"total\":1," + "  \"hits\":["
                + "    \"_index\":\"dataIndex\"" + "  ]" + "}" + "}";

        String endResponse4 = "{" + "\"_scroll_id\":\"scrollId_8\"," + "\"hits\":[]" + "}";

        String response5 = "{" + "\"_scroll_id\":\"scrollId_9\"," + "\"hits\":{" + "  \"total\":0," + "  \"hits\":[]" + "}" + "}";

        String finalDataSummaryResponse = "{" + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":0,"
                + "  \"successful\":0," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":0," + "  \"max_score\":null,"
                + "  \"hits\":[]" + "}," + "\"aggregations\":{" + "\"earliestTime\":{" + "\"value\": null" + "}," + "\"latestTime\":{"
                + "\"value\": null" + "}" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(dataSummaryResponse), 200),
                new HttpResponse(toStream(indexResponse), 200), new HttpResponse(toStream(initialResponse1), 200),
                new HttpResponse(toStream(continueResponse1), 200), new HttpResponse(toStream(endResponse1), 200),
                new HttpResponse(toStream(initialResponse2), 200), new HttpResponse(toStream(endResponse2), 200),
                new HttpResponse(toStream(initialResponse3), 200), new HttpResponse(toStream(dataSummaryResponse2), 200),
                new HttpResponse(toStream(indexResponse), 200), new HttpResponse(toStream(initialResponse4), 200),
                new HttpResponse(toStream(endResponse4), 200), new HttpResponse(toStream(response5), 200),
                new HttpResponse(toStream(finalDataSummaryResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000000L, 1407200000000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertEquals(initialResponse1, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertEquals(continueResponse1, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertEquals(initialResponse2, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertEquals(initialResponse4, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());

        requester.assertEqualRequestsToResponses();
        requester.assertResponsesHaveBeenConsumed();

        int requestCount = 0;
        RequestParams requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/index-*/dataType/_search?size=1", requestParams.url);
        String expectedDataSummaryBody = "{" + "  \"sort\": [{\"_doc\":{\"order\":\"asc\"}}]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T16:53:20.000Z\"," + "              \"lt\": \"2014-08-05T00:53:20.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  },"
                + "  \"aggs\":{" + "    \"earliestTime\":{" + "      \"min\":{\"field\":\"time\"}" + "    }," + "    \"latestTime\":{"
                + "      \"max\":{\"field\":\"time\"}" + "    }" + "  }" + "}";
        assertEquals(expectedDataSummaryBody.replace(" ", ""), requestParams.requestBody.replace(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/dataIndex/_settings", requestParams.url);
        assertNull(requestParams.requestBody);

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", requestParams.url);
        String expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {"
                + "    \"bool\": {" + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {"
                + "            \"time\": {" + "              \"gte\": \"2014-05-13T16:53:21.000Z\","
                + "              \"lt\": \"2014-05-13T17:53:21.000Z\"," + "              \"format\": \"date_time\"" + "            }"
                + "          }" + "        }" + "      ]" + "    }" + "  }" + "}";
        assertEquals(expectedSearchBody.replaceAll(" ", ""), requestParams.requestBody.replaceAll(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", requestParams.url);
        assertEquals("scrollId_1", requestParams.requestBody);

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", requestParams.url);
        assertEquals("scrollId_2", requestParams.requestBody);

        requestParams = requester.getGetRequestParams(requestCount++);
        expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T17:53:21.000Z\"," + "              \"lt\": \"2014-05-13T18:53:21.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  }"
                + "}";
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", requestParams.url);
        assertEquals(expectedSearchBody.replaceAll(" ", ""), requestParams.requestBody.replaceAll(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", requestParams.url);
        assertEquals("scrollId_4", requestParams.requestBody);

        requestParams = requester.getGetRequestParams(requestCount++);
        expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T18:53:21.000Z\"," + "              \"lt\": \"2014-05-13T19:53:21.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  }"
                + "}";
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", requestParams.url);
        assertEquals(expectedSearchBody.replaceAll(" ", ""), requestParams.requestBody.replaceAll(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/index-*/dataType/_search?size=1", requestParams.url);
        expectedDataSummaryBody = "{" + "  \"sort\": [{\"_doc\":{\"order\":\"asc\"}}]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T19:53:21.000Z\"," + "              \"lt\": \"2014-08-05T00:53:20.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  },"
                + "  \"aggs\":{" + "    \"earliestTime\":{" + "      \"min\":{\"field\":\"time\"}" + "    }," + "    \"latestTime\":{"
                + "      \"max\":{\"field\":\"time\"}" + "    }" + "  }" + "}";
        assertEquals(expectedDataSummaryBody.replace(" ", ""), requestParams.requestBody.replace(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/dataIndex/_settings", requestParams.url);
        assertNull(requestParams.requestBody);

        requestParams = requester.getGetRequestParams(requestCount++);
        expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T18:53:21.000Z\"," + "              \"lt\": \"2014-05-13T18:53:31.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  }"
                + "}";
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", requestParams.url);
        assertEquals(expectedSearchBody.replaceAll(" ", ""), requestParams.requestBody.replaceAll(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", requestParams.url);
        assertEquals("scrollId_7", requestParams.requestBody);

        requestParams = requester.getGetRequestParams(requestCount++);
        expectedSearchBody = "{" + "  \"sort\": [" + "    {\"time\": {\"order\": \"asc\"}}" + "  ]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T18:53:31.000Z\"," + "              \"lt\": \"2014-05-13T18:53:41.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  }"
                + "}";
        assertEquals("http://localhost:9200/index-*/dataType/_search?scroll=60m&size=1000", requestParams.url);
        assertEquals(expectedSearchBody.replaceAll(" ", ""), requestParams.requestBody.replaceAll(" ", ""));

        requestParams = requester.getGetRequestParams(requestCount++);
        assertEquals("http://localhost:9200/index-*/dataType/_search?size=1", requestParams.url);
        expectedDataSummaryBody = "{" + "  \"sort\": [{\"_doc\":{\"order\":\"asc\"}}]," + "  \"query\": {" + "    \"bool\": {"
                + "      \"filter\": [" + "        {\"match_all\":{}}," + "        {" + "          \"range\": {" + "            \"time\": {"
                + "              \"gte\": \"2014-05-13T18:53:41.000Z\"," + "              \"lt\": \"2014-08-05T00:53:20.000Z\","
                + "              \"format\": \"date_time\"" + "            }" + "          }" + "        }" + "      ]" + "    }" + "  },"
                + "  \"aggs\":{" + "    \"earliestTime\":{" + "      \"min\":{\"field\":\"time\"}" + "    }," + "    \"latestTime\":{"
                + "      \"max\":{\"field\":\"time\"}" + "    }" + "  }" + "}";
        assertEquals(expectedDataSummaryBody.replace(" ", ""), requestParams.requestBody.replace(" ", ""));

        assertEquals(requestCount, requester.requestCount);

        String[] deletedScrollIds = { "scrollId_3", "scrollId_5", "scrollId_6", "scrollId_8", "scrollId_9" };
        assertEquals(5, requester.deleteRequestParams.size());
        for (int i = 0; i < deletedScrollIds.length; i++) {
            assertEquals("http://localhost:9200/_search/scroll", requester.getDeleteRequestParams(i).url);
            assertEquals(String.format(Locale.ROOT, "{\"scroll_id\":[\"%s\"]}", deletedScrollIds[i]),
                    requester.getDeleteRequestParams(i).requestBody);
        }
    }

    public void testChunkedDataExtraction_GivenZeroHits() throws IOException {
        String dataSummaryResponse = "{" + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1,"
                + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":0," + "  \"max_score\":null,"
                + "  \"hits\":[]" + "}," + "\"aggregations\":{" + "\"earliestTime\":null," + "\"latestTime\":null" + "}" + "}";

        String searchResponse = "{" + "\"_scroll_id\":\"scrollId_1\"," + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":0,"
                + "  \"max_score\":null," + "  \"hits\":[]" + "}" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(dataSummaryResponse), 200),
                new HttpResponse(toStream(searchResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000000L, 1407200000000L, jobLogger);

        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());
    }

    public void testChunkedDataExtraction_GivenDataSummaryRequestIsNotOk() throws IOException {
        String dataSummaryResponse = "{}";
        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(dataSummaryResponse), 400));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        IOException e = expectThrows(IOException.class, () -> extractor.newSearch(1400000000000L, 1407200000000L, jobLogger));
        assertEquals("Request 'http://localhost:9200/index-*/dataType/_search?size=1' " + "failed with status code: 400. Response was:\n"
                + dataSummaryResponse, e.getMessage());
    }

    public void testChunkedDataExtraction_GivenEmptyDataSummaryResponse() throws IOException {
        String dataSummaryResponse = "{}";
        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(dataSummaryResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        IOException e = expectThrows(IOException.class, () -> extractor.newSearch(1400000000000L, 1407200000000L, jobLogger));
        assertEquals("Failed to parse string from pattern '\"hits\":\\{.*?\"total\":(.*?),'. Response was:\n" + dataSummaryResponse,
                e.getMessage());
    }

    public void testChunkedDataExtraction_GivenTotalHitsCannotBeParsed() throws IOException {
        String dataSummaryResponse = "{" + "\"hits\":{" + "  \"total\":\"NaN\"," + "  \"max_score\":null," + "  \"hits\":[]" + "}," + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(dataSummaryResponse), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        IOException e = expectThrows(IOException.class, () -> extractor.newSearch(1400000000000L, 1407200000000L, jobLogger));
        assertEquals("Failed to parse long from pattern '\"hits\":\\{.*?\"total\":(.*?),'. Response was:\n" + dataSummaryResponse,
                e.getMessage());
    }

    public void testCancel_GivenChunked() throws IOException {
        String dataSummaryResponse = "{" + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{" + "  \"total\":1,"
                + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000," + "  \"max_score\":null,"
                + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\"," + "    \"_id\":\"1403481600\","
                + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }" + "  ]" + "},"
                + "\"aggregations\":{" + "\"earliestTime\":{" + "\"value\":1400000001000," + "\"value_as_string\":\"2014-05-13T16:53:21Z\""
                + "}," + "\"latestTime\":{" + "\"value\":1400007201000," + "\"value_as_string\":\"2014-05-13T17:16:01Z\"" + "}" + "}" + "}";

        String indexResponse = "{" + "\"dataIndex\":{" + "  \"settings\":{" + "    \"index\":{" + "      \"creation_date\":0,"
                + "      \"number_of_shards\":\"5\"," + "      \"number_of_replicas\":\"1\"" + "    }" + "  }" + "}";

        String initialResponse1 = "{" + "\"_scroll_id\":\"scrollId_1\"," + "\"took\":17," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403481600\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403481600\"" + "    }"
                + "  ]" + "}" + "}";

        String continueResponse1 = "{" + "\"_scroll_id\":\"scrollId_2\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000,"
                + "  \"max_score\":null," + "  \"hits\":[" + "    \"_index\":\"dataIndex\"," + "    \"_type\":\"dataType\","
                + "    \"_id\":\"1403782200\"," + "    \"_score\":null," + "    \"_source\":{" + "      \"id\":\"1403782200\"" + "    }"
                + "  ]" + "}" + "}";

        String endResponse1 = "{" + "\"_scroll_id\":\"scrollId_3\"," + "\"took\":8," + "\"timed_out\":false," + "\"_shards\":{"
                + "  \"total\":1," + "  \"successful\":1," + "  \"failed\":0" + "}," + "\"hits\":{" + "  \"total\":10000,"
                + "  \"max_score\":null," + "  \"hits\":[]" + "}" + "}";

        String initialResponse2 = "{" + "\"_scroll_id\":\"scrollId_4\"," + "\"hits\":{" + "  \"total\":10000," + "  \"hits\":["
                + "    \"_index\":\"dataIndex\"" + "  ]" + "}" + "}";

        String endResponse2 = "{" + "\"_scroll_id\":\"scrollId_5\"," + "\"hits\":[]" + "}";

        List<HttpResponse> responses = Arrays.asList(new HttpResponse(toStream(dataSummaryResponse), 200),
                new HttpResponse(toStream(indexResponse), 200), new HttpResponse(toStream(initialResponse1), 200),
                new HttpResponse(toStream(continueResponse1), 200), new HttpResponse(toStream(endResponse1), 200),
                new HttpResponse(toStream(initialResponse2), 200), new HttpResponse(toStream(endResponse2), 200));

        MockHttpRequester requester = new MockHttpRequester(responses);
        createExtractor(requester);

        extractor.newSearch(1400000000000L, 1407200000000L, jobLogger);

        extractor.cancel();

        assertTrue(extractor.hasNext());
        assertEquals(initialResponse1, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertEquals(continueResponse1, streamToString(extractor.next().get()));
        assertTrue(extractor.hasNext());
        assertFalse(extractor.next().isPresent());
        assertFalse(extractor.hasNext());

        assertEquals("http://localhost:9200/_search/scroll", requester.getDeleteRequestParams(0).url);
        assertEquals("{\"scroll_id\":[\"scrollId_3\"]}", requester.getDeleteRequestParams(0).requestBody);
        assertEquals(1, requester.deleteRequestParams.size());

        extractor.newSearch(1407200000000L, 1407203600000L, jobLogger);
        assertTrue(extractor.hasNext());
    }

    private static InputStream toStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }

    private static String streamToString(InputStream stream) throws IOException {
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return buffer.lines().collect(Collectors.joining("\n")).trim();
        }
    }

    private void createExtractor(MockHttpRequester httpRequester) {
        ElasticsearchQueryBuilder queryBuilder = new ElasticsearchQueryBuilder(SEARCH, aggregations, scriptFields, fields, TIME_FIELD);
        ElasticsearchUrlBuilder urlBuilder = ElasticsearchUrlBuilder.create(BASE_URL, INDEXES, TYPES);
        extractor = new ElasticsearchDataExtractor(httpRequester, urlBuilder, queryBuilder, 1000);
    }

    private static class MockHttpRequester extends HttpRequester {
        private List<HttpResponse> getResponses;
        private List<HttpResponse> deleteResponses;
        private int requestCount = 0;
        private List<RequestParams> getRequestParams;
        private List<RequestParams> deleteRequestParams;

        public MockHttpRequester(List<HttpResponse> responses) {
            getResponses = responses;
            deleteResponses = new ArrayList<>();
            getRequestParams = new ArrayList<>(responses.size());
            deleteRequestParams = new ArrayList<>();
        }

        @Override
        public HttpResponse get(String url, String requestBody) {
            getRequestParams.add(new RequestParams(url, requestBody));
            return getResponses.get(requestCount++);
        }

        @Override
        public HttpResponse delete(String url, String requestBody) {
            deleteRequestParams.add(new RequestParams(url, requestBody));
            HttpResponse response = new HttpResponse(toStream(CLEAR_SCROLL_RESPONSE), 200);
            deleteResponses.add(response);
            return response;
        }

        public RequestParams getGetRequestParams(int callCount) {
            return getRequestParams.get(callCount);
        }

        public RequestParams getDeleteRequestParams(int callCount) {
            return deleteRequestParams.get(callCount);
        }

        public void assertEqualRequestsToResponses() {
            assertEquals(getResponses.size(), getRequestParams.size());
        }

        public void assertResponsesHaveBeenConsumed() throws IOException {
            for (HttpResponse response : getResponses) {
                assertEquals(0, response.getStream().available());
            }
            for (HttpResponse response : deleteResponses) {
                assertEquals(0, response.getStream().available());
            }
        }
    }

    private static class RequestParams {
        public final String url;
        public final String requestBody;

        public RequestParams(String url, String requestBody) {
            this.url = url;
            this.requestBody = requestBody;
        }
    }
}
