/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.indexlifecycle.AllocateAction;
import org.elasticsearch.client.indexlifecycle.DeleteAction;
import org.elasticsearch.client.indexlifecycle.ForceMergeAction;
import org.elasticsearch.client.indexlifecycle.LifecycleAction;
import org.elasticsearch.client.indexlifecycle.ReadOnlyAction;
import org.elasticsearch.client.indexlifecycle.RolloverAction;
import org.elasticsearch.client.indexlifecycle.ShrinkAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.index.rankeval.DiscountedCumulativeGain;
import org.elasticsearch.index.rankeval.EvaluationMetric;
import org.elasticsearch.index.rankeval.ExpectedReciprocalRank;
import org.elasticsearch.index.rankeval.MeanReciprocalRank;
import org.elasticsearch.index.rankeval.MetricDetail;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestHighLevelClientTests extends ESTestCase {

    private static final String SUBMIT_TASK_PREFIX = "submit_";
    private static final String SUBMIT_TASK_SUFFIX = "_task";
    private static final ProtocolVersion HTTP_PROTOCOL = new ProtocolVersion("http", 1, 1);
    private static final RequestLine REQUEST_LINE = new BasicRequestLine(HttpGet.METHOD_NAME, "/", HTTP_PROTOCOL);

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() {
        restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());
    }

    public void testCloseIsIdempotent() throws IOException {
        restHighLevelClient.close();
        verify(restClient, times(1)).close();
        restHighLevelClient.close();
        verify(restClient, times(2)).close();
        restHighLevelClient.close();
        verify(restClient, times(3)).close();
    }

    public void testPingSuccessful() throws IOException {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.OK));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
        assertTrue(restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testPing404NotFound() throws IOException {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.NOT_FOUND));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
        assertFalse(restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testPingSocketTimeout() throws IOException {
        when(restClient.performRequest(any(Request.class))).thenThrow(new SocketTimeoutException());
        expectThrows(SocketTimeoutException.class, () -> restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testInfo() throws IOException {
        MainResponse testInfo = new MainResponse("nodeName", Version.CURRENT, new ClusterName("clusterName"), "clusterUuid",
                Build.CURRENT);
        mockResponse(testInfo);
        MainResponse receivedInfo = restHighLevelClient.info(RequestOptions.DEFAULT);
        assertEquals(testInfo, receivedInfo);
    }

    public void testSearchScroll() throws IOException {
        SearchResponse mockSearchResponse = new SearchResponse(new SearchResponseSections(SearchHits.empty(), InternalAggregations.EMPTY,
                null, false, false, null, 1), randomAlphaOfLengthBetween(5, 10), 5, 5, 0, 100, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
        mockResponse(mockSearchResponse);
        SearchResponse searchResponse = restHighLevelClient.scroll(
                new SearchScrollRequest(randomAlphaOfLengthBetween(5, 10)), RequestOptions.DEFAULT);
        assertEquals(mockSearchResponse.getScrollId(), searchResponse.getScrollId());
        assertEquals(0, searchResponse.getHits().getTotalHits().value);
        assertEquals(5, searchResponse.getTotalShards());
        assertEquals(5, searchResponse.getSuccessfulShards());
        assertEquals(100, searchResponse.getTook().getMillis());
    }

    public void testClearScroll() throws IOException {
        ClearScrollResponse mockClearScrollResponse = new ClearScrollResponse(randomBoolean(), randomIntBetween(0, Integer.MAX_VALUE));
        mockResponse(mockClearScrollResponse);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(5, 10));
        ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        assertEquals(mockClearScrollResponse.isSucceeded(), clearScrollResponse.isSucceeded());
        assertEquals(mockClearScrollResponse.getNumFreed(), clearScrollResponse.getNumFreed());
    }

    private void mockResponse(ToXContent toXContent) throws IOException {
        Response response = mock(Response.class);
        ContentType contentType = ContentType.parse(RequestConverters.REQUEST_BODY_CONTENT_TYPE.mediaType());
        String requestBody = toXContent(toXContent, RequestConverters.REQUEST_BODY_CONTENT_TYPE, false).utf8ToString();
        when(response.getEntity()).thenReturn(new NStringEntity(requestBody, contentType));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
    }

    public void testRequestValidation() {
        ActionRequestValidationException validationException = new ActionRequestValidationException();
        validationException.addValidationError("validation error");
        ActionRequest request = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return validationException;
            }
        };

        {
            ActionRequestValidationException actualException = expectThrows(ActionRequestValidationException.class,
                    () -> restHighLevelClient.performRequest(request, null, RequestOptions.DEFAULT, null, null));
            assertSame(validationException, actualException);
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            restHighLevelClient.performRequestAsync(request, null, RequestOptions.DEFAULT, null, trackingActionListener, null);
            assertSame(validationException, trackingActionListener.exception.get());
        }
    }

    public void testParseEntity() throws IOException {
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> restHighLevelClient.parseEntity(null, null));
            assertEquals("Response body expected but not returned", ise.getMessage());
        }
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class,
                    () -> restHighLevelClient.parseEntity(new StringEntity("", (ContentType) null), null));
            assertEquals("Elasticsearch didn't return the [Content-Type] header, unable to parse response body", ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("", ContentType.APPLICATION_SVG_XML);
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> restHighLevelClient.parseEntity(entity, null));
            assertEquals("Unsupported Content-Type: " + entity.getContentType().getValue(), ise.getMessage());
        }
        {
            CheckedFunction<XContentParser, String, IOException> entityParser = parser -> {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertTrue(parser.nextToken().isValue());
                String value = parser.text();
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                return value;
            };
            HttpEntity jsonEntity = new StringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON);
            assertEquals("value", restHighLevelClient.parseEntity(jsonEntity, entityParser));
            HttpEntity yamlEntity = new StringEntity("---\nfield: value\n", ContentType.create("application/yaml"));
            assertEquals("value", restHighLevelClient.parseEntity(yamlEntity, entityParser));
            HttpEntity smileEntity = createBinaryEntity(SmileXContent.contentBuilder(), ContentType.create("application/smile"));
            assertEquals("value", restHighLevelClient.parseEntity(smileEntity, entityParser));
            HttpEntity cborEntity = createBinaryEntity(CborXContent.contentBuilder(), ContentType.create("application/cbor"));
            assertEquals("value", restHighLevelClient.parseEntity(cborEntity, entityParser));
        }
    }

    private static HttpEntity createBinaryEntity(XContentBuilder xContentBuilder, ContentType contentType) throws IOException {
        try (XContentBuilder builder = xContentBuilder) {
            builder.startObject();
            builder.field("field", "value");
            builder.endObject();
            return new ByteArrayEntity(BytesReference.bytes(builder).toBytesRef().bytes, contentType);
        }
    }

    public void testConvertExistsResponse() {
        RestStatus restStatus = randomBoolean() ? RestStatus.OK : randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        boolean result = RestHighLevelClient.convertExistsResponse(response);
        assertEquals(restStatus == RestStatus.OK, result);
    }

    public void testParseResponseException() throws IOException {
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertSame(responseException, elasticsearchException.getCause());
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
                    ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            httpResponse.setEntity(new StringEntity("{\"error\":", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals("Unable to parse response body", elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertSame(responseException, elasticsearchException.getCause());
            assertThat(elasticsearchException.getSuppressed()[0], instanceOf(IOException.class));
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            httpResponse.setEntity(new StringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals("Unable to parse response body", elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertSame(responseException, elasticsearchException.getCause());
            assertThat(elasticsearchException.getSuppressed()[0], instanceOf(IllegalStateException.class));
        }
    }

    public void testPerformRequestOnSuccess() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        when(restClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        {
            Integer result = restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                    response -> response.getStatusLine().getStatusCode(), Collections.emptySet());
            assertEquals(restStatus.getStatus(), result.intValue());
        }
        {
            IOException ioe = expectThrows(IOException.class, () -> restHighLevelClient.performRequest(mainRequest,
                    requestConverter, RequestOptions.DEFAULT, response -> {throw new IllegalStateException();}, Collections.emptySet()));
            assertEquals("Unable to parse response body for Response{requestLine=GET / http/1.1, host=http://localhost:9200, " +
                    "response=http/1.1 " + restStatus.getStatus() + " " + restStatus.name() + "}", ioe.getMessage());
        }
    }

    public void testPerformRequestOnResponseExceptionWithoutEntity() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                        response -> response.getStatusLine().getStatusCode(), Collections.emptySet()));
        assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getCause());
    }

    public void testPerformRequestOnResponseExceptionWithEntity() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
                ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                        response -> response.getStatusLine().getStatusCode(), Collections.emptySet()));
        assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
    }

    public void testPerformRequestOnResponseExceptionWithBrokenEntity() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        httpResponse.setEntity(new StringEntity("{\"error\":", ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                        response -> response.getStatusLine().getStatusCode(), Collections.emptySet()));
        assertEquals("Unable to parse response body", elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getCause());
        assertThat(elasticsearchException.getSuppressed()[0], instanceOf(JsonParseException.class));
    }

    public void testPerformRequestOnResponseExceptionWithBrokenEntity2() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        httpResponse.setEntity(new StringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                        response -> response.getStatusLine().getStatusCode(), Collections.emptySet()));
        assertEquals("Unable to parse response body", elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getCause());
        assertThat(elasticsearchException.getSuppressed()[0], instanceOf(IllegalStateException.class));
    }

    public void testPerformRequestOnResponseExceptionWithIgnores() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(RestStatus.NOT_FOUND));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        //although we got an exception, we turn it into a successful response because the status code was provided among ignores
        assertEquals(Integer.valueOf(404), restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(), Collections.singleton(404)));
    }

    public void testPerformRequestOnResponseExceptionWithIgnoresErrorNoBody() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(RestStatus.NOT_FOUND));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                        response -> {throw new IllegalStateException();}, Collections.singleton(404)));
        assertEquals(RestStatus.NOT_FOUND, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getCause());
        assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
    }

    public void testPerformRequestOnResponseExceptionWithIgnoresErrorValidBody() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(RestStatus.NOT_FOUND));
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":404}",
                ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                        response -> {throw new IllegalStateException();}, Collections.singleton(404)));
        assertEquals(RestStatus.NOT_FOUND, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
    }

    public void testWrapResponseListenerOnSuccess() {
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                    response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            responseListener.onSuccess(new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse));
            assertNull(trackingActionListener.exception.get());
            assertEquals(restStatus.getStatus(), trackingActionListener.statusCode.get());
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                    response -> {throw new IllegalStateException();}, trackingActionListener, Collections.emptySet());
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            responseListener.onSuccess(new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse));
            assertThat(trackingActionListener.exception.get(), instanceOf(IOException.class));
            IOException ioe = (IOException) trackingActionListener.exception.get();
            assertEquals("Unable to parse response body for Response{requestLine=GET / http/1.1, host=http://localhost:9200, " +
                    "response=http/1.1 " + restStatus.getStatus() + " " + restStatus.name() + "}", ioe.getMessage());
            assertThat(ioe.getCause(), instanceOf(IllegalStateException.class));
        }
    }

    public void testWrapResponseListenerOnException() {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
        IllegalStateException exception = new IllegalStateException();
        responseListener.onFailure(exception);
        assertSame(exception, trackingActionListener.exception.get());
    }

    public void testWrapResponseListenerOnResponseExceptionWithoutEntity() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException) trackingActionListener.exception.get();
        assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getCause());
    }

    public void testWrapResponseListenerOnResponseExceptionWithEntity() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
                ContentType.APPLICATION_JSON));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
        assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
    }

    public void testWrapResponseListenerOnResponseExceptionWithBrokenEntity() throws IOException {
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                    response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            httpResponse.setEntity(new StringEntity("{\"error\":", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            responseListener.onFailure(responseException);
            assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
            ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
            assertEquals("Unable to parse response body", elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertSame(responseException, elasticsearchException.getCause());
            assertThat(elasticsearchException.getSuppressed()[0], instanceOf(JsonParseException.class));
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                    response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            httpResponse.setEntity(new StringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            responseListener.onFailure(responseException);
            assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
            ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
            assertEquals("Unable to parse response body", elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertSame(responseException, elasticsearchException.getCause());
            assertThat(elasticsearchException.getSuppressed()[0], instanceOf(IllegalStateException.class));
        }
    }

    public void testWrapResponseListenerOnResponseExceptionWithIgnores() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.singleton(404));
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(RestStatus.NOT_FOUND));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        //although we got an exception, we turn it into a successful response because the status code was provided among ignores
        assertNull(trackingActionListener.exception.get());
        assertEquals(404, trackingActionListener.statusCode.get());
    }

    public void testWrapResponseListenerOnResponseExceptionWithIgnoresErrorNoBody() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        //response parsing throws exception while handling ignores. same as when GetResponse#fromXContent throws error when trying
        //to parse a 404 response which contains an error rather than a valid document not found response.
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> { throw new IllegalStateException(); }, trackingActionListener, Collections.singleton(404));
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(RestStatus.NOT_FOUND));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
        assertEquals(RestStatus.NOT_FOUND, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getCause());
        assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
    }

    public void testWrapResponseListenerOnResponseExceptionWithIgnoresErrorValidBody() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        //response parsing throws exception while handling ignores. same as when GetResponse#fromXContent throws error when trying
        //to parse a 404 response which contains an error rather than a valid document not found response.
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> { throw new IllegalStateException(); }, trackingActionListener, Collections.singleton(404));
        HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(RestStatus.NOT_FOUND));
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":404}",
                ContentType.APPLICATION_JSON));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
        assertEquals(RestStatus.NOT_FOUND, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
    }

    public void testDefaultNamedXContents() {
        List<NamedXContentRegistry.Entry> namedXContents = RestHighLevelClient.getDefaultNamedXContents();
        int expectedInternalAggregations = InternalAggregationTestCase.getDefaultNamedXContents().size();
        int expectedSuggestions = 3;
        assertEquals(expectedInternalAggregations + expectedSuggestions, namedXContents.size());
        Map<Class<?>, Integer> categories = new HashMap<>();
        for (NamedXContentRegistry.Entry namedXContent : namedXContents) {
            Integer counter = categories.putIfAbsent(namedXContent.categoryClass, 1);
            if (counter != null) {
                categories.put(namedXContent.categoryClass, counter + 1);
            }
        }
        assertEquals(2, categories.size());
        assertEquals(expectedInternalAggregations, categories.get(Aggregation.class).intValue());
        assertEquals(expectedSuggestions, categories.get(Suggest.Suggestion.class).intValue());
    }

    public void testProvidedNamedXContents() {
        List<NamedXContentRegistry.Entry> namedXContents = RestHighLevelClient.getProvidedNamedXContents();
        assertEquals(17, namedXContents.size());
        Map<Class<?>, Integer> categories = new HashMap<>();
        List<String> names = new ArrayList<>();
        for (NamedXContentRegistry.Entry namedXContent : namedXContents) {
            names.add(namedXContent.name.getPreferredName());
            Integer counter = categories.putIfAbsent(namedXContent.categoryClass, 1);
            if (counter != null) {
                categories.put(namedXContent.categoryClass, counter + 1);
            }
        }
        assertEquals("Had: " + categories, 4, categories.size());
        assertEquals(Integer.valueOf(3), categories.get(Aggregation.class));
        assertTrue(names.contains(ChildrenAggregationBuilder.NAME));
        assertTrue(names.contains(MatrixStatsAggregationBuilder.NAME));
        assertEquals(Integer.valueOf(4), categories.get(EvaluationMetric.class));
        assertTrue(names.contains(PrecisionAtK.NAME));
        assertTrue(names.contains(DiscountedCumulativeGain.NAME));
        assertTrue(names.contains(MeanReciprocalRank.NAME));
        assertTrue(names.contains(ExpectedReciprocalRank.NAME));
        assertEquals(Integer.valueOf(4), categories.get(MetricDetail.class));
        assertTrue(names.contains(PrecisionAtK.NAME));
        assertTrue(names.contains(MeanReciprocalRank.NAME));
        assertTrue(names.contains(DiscountedCumulativeGain.NAME));
        assertTrue(names.contains(ExpectedReciprocalRank.NAME));
        assertEquals(Integer.valueOf(6), categories.get(LifecycleAction.class));
        assertTrue(names.contains(AllocateAction.NAME));
        assertTrue(names.contains(DeleteAction.NAME));
        assertTrue(names.contains(ForceMergeAction.NAME));
        assertTrue(names.contains(ReadOnlyAction.NAME));
        assertTrue(names.contains(RolloverAction.NAME));
        assertTrue(names.contains(ShrinkAction.NAME));
    }

    public void testApiNamingConventions() throws Exception {
        //this list should be empty once the high-level client is feature complete
        String[] notYetSupportedApi = new String[]{
            "cluster.remote_info",
            "create",
            "get_source",
            "indices.delete_alias",
            "indices.exists_template",
            "indices.exists_type",
            "indices.get_upgrade",
            "indices.put_alias",
            "render_search_template",
            "scripts_painless_execute"
        };
        //These API are not required for high-level client feature completeness
        String[] notRequiredApi = new String[] {
            "cluster.allocation_explain",
            "cluster.pending_tasks",
            "cluster.reroute",
            "cluster.state",
            "cluster.stats",
            "indices.shard_stores",
            "indices.upgrade",
            "indices.recovery",
            "indices.segments",
            "indices.stats",
            "ingest.processor_grok",
            "nodes.info",
            "nodes.stats",
            "nodes.hot_threads",
            "nodes.usage",
            "nodes.reload_secure_settings",
            "search_shards",
        };
        Set<String> deprecatedMethods = new HashSet<>();
        deprecatedMethods.add("indices.force_merge");
        deprecatedMethods.add("multi_get");
        deprecatedMethods.add("multi_search");
        deprecatedMethods.add("search_scroll");

        ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load("/rest-api-spec/api");
        Set<String> apiSpec = restSpec.getApis().stream().map(ClientYamlSuiteRestApi::getName).collect(Collectors.toSet());

        Set<String> topLevelMethodsExclusions = new HashSet<>();
        topLevelMethodsExclusions.add("getLowLevelClient");
        topLevelMethodsExclusions.add("close");

        Map<String, Method> methods = Arrays.stream(RestHighLevelClient.class.getMethods())
                .filter(method -> method.getDeclaringClass().equals(RestHighLevelClient.class)
                        && topLevelMethodsExclusions.contains(method.getName()) == false)
                .map(method -> Tuple.tuple(toSnakeCase(method.getName()), method))
                .flatMap(tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                        ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType()) : Stream.of(tuple))
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        Set<String> apiNotFound = new HashSet<>();

        for (Map.Entry<String, Method> entry : methods.entrySet()) {
            Method method = entry.getValue();
            String apiName = entry.getKey();

            assertTrue("method [" + apiName + "] is not final",
                    Modifier.isFinal(method.getClass().getModifiers()) || Modifier.isFinal(method.getModifiers()));
            assertTrue("method [" + method + "] should be public", Modifier.isPublic(method.getModifiers()));

            //we convert all the method names to snake case, hence we need to look for the '_async' suffix rather than 'Async'
            if (apiName.endsWith("_async")) {
                assertAsyncMethod(methods, method, apiName);
            } else if (isSubmitTaskMethod(apiName)) {
                assertSubmitTaskMethod(methods, method, apiName, restSpec);
            } else {
                assertSyncMethod(method, apiName);
                boolean remove = apiSpec.remove(apiName);
                if (remove == false) {
                    if (deprecatedMethods.contains(apiName)) {
                        assertTrue("method [" + method.getName() + "], api [" + apiName + "] should be deprecated",
                            method.isAnnotationPresent(Deprecated.class));
                    } else {
                        //TODO xpack api are currently ignored, we need to load xpack yaml spec too
                        if (apiName.startsWith("xpack.") == false &&
                            apiName.startsWith("license.") == false &&
                            apiName.startsWith("machine_learning.") == false &&
                            apiName.startsWith("rollup.") == false &&
                            apiName.startsWith("watcher.") == false &&
                            apiName.startsWith("graph.") == false &&
                            apiName.startsWith("migration.") == false &&
                            apiName.startsWith("security.") == false &&
                            apiName.startsWith("index_lifecycle.") == false &&
                            apiName.startsWith("ccr.") == false &&
                            apiName.endsWith("freeze") == false) {
                            apiNotFound.add(apiName);
                        }
                    }
                }
            }
        }
        assertThat("Some client method doesn't match a corresponding API defined in the REST spec: " + apiNotFound,
            apiNotFound.size(), equalTo(0));

        //we decided not to support cat API in the high-level REST client, they are supposed to be used from a low-level client
        apiSpec.removeIf(api -> api.startsWith("cat."));
        Stream.concat(Arrays.stream(notYetSupportedApi), Arrays.stream(notRequiredApi)).forEach(
            api -> assertTrue(api + " API is either not defined in the spec or already supported by the high-level client",
                apiSpec.remove(api)));
        assertThat("Some API are not supported but they should be: " + apiSpec, apiSpec.size(), equalTo(0));
    }

    private static void assertSyncMethod(Method method, String apiName) {
        //A few methods return a boolean rather than a response object
        if (apiName.equals("ping") || apiName.contains("exist")) {
            assertThat("the return type for method [" + method + "] is incorrect",
                method.getReturnType().getSimpleName(), equalTo("boolean"));
        } else {
            // It's acceptable for 404s to be represented as empty Optionals 
            if (!method.getReturnType().isAssignableFrom(Optional.class)) {
                assertThat("the return type for method [" + method + "] is incorrect",
                    method.getReturnType().getSimpleName(), endsWith("Response"));
            }
        }

        assertEquals("incorrect number of exceptions for method [" + method + "]", 1, method.getExceptionTypes().length);
        //a few methods don't accept a request object as argument
        if (apiName.equals("ping") || apiName.equals("info") || apiName.equals("security.get_ssl_certificates")
            || apiName.equals("security.authenticate") || apiName.equals("license.get_trial_status")
            || apiName.equals("license.get_basic_status")) {
            assertEquals("incorrect number of arguments for method [" + method + "]", 1, method.getParameterTypes().length);
            assertThat("the parameter to method [" + method + "] is the wrong type",
                method.getParameterTypes()[0], equalTo(RequestOptions.class));
        } else {
            assertEquals("incorrect number of arguments for method [" + method + "]", 2, method.getParameterTypes().length);
            assertThat("the first parameter to method [" + method + "] is the wrong type",
                method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
            assertThat("the second parameter to method [" + method + "] is the wrong type",
                method.getParameterTypes()[1], equalTo(RequestOptions.class));
        }
    }

    private static void assertAsyncMethod(Map<String, Method> methods, Method method, String apiName) {
        assertTrue("async method [" + method.getName() + "] doesn't have corresponding sync method",
                methods.containsKey(apiName.substring(0, apiName.length() - 6)));
        assertThat("async method [" + method + "] should return void", method.getReturnType(), equalTo(Void.TYPE));
        assertEquals("async method [" + method + "] should not throw any exceptions", 0, method.getExceptionTypes().length);
        if (apiName.equals("security.authenticate_async") || apiName.equals("security.get_ssl_certificates_async")) {
            assertEquals(2, method.getParameterTypes().length);
            assertThat(method.getParameterTypes()[0], equalTo(RequestOptions.class));
            assertThat(method.getParameterTypes()[1], equalTo(ActionListener.class));
        } else {
            assertEquals("async method [" + method + "] has the wrong number of arguments", 3, method.getParameterTypes().length);
            assertThat("the first parameter to async method [" + method + "] should be a request type",
                method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
            assertThat("the second parameter to async method [" + method + "] is the wrong type",
                method.getParameterTypes()[1], equalTo(RequestOptions.class));
            assertThat("the third parameter to async method [" + method + "] is the wrong type",
                method.getParameterTypes()[2], equalTo(ActionListener.class));
        }
    }

    private static void assertSubmitTaskMethod(Map<String, Method> methods, Method method, String apiName,
                                               ClientYamlSuiteRestSpec restSpec) {
        String methodName = extractMethodName(apiName);
        assertTrue("submit task method [" + method.getName() + "] doesn't have corresponding sync method",
            methods.containsKey(methodName));
        assertEquals("submit task method [" + method + "] has the wrong number of arguments", 2, method.getParameterTypes().length);
        assertThat("the first parameter to submit task method [" + method + "] is the wrong type",
            method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
        assertThat("the second parameter to submit task method [" + method + "] is the wrong type",
            method.getParameterTypes()[1], equalTo(RequestOptions.class));

        assertThat("submit task method [" + method + "] must have wait_for_completion parameter in rest spec",
            restSpec.getApi(methodName).getParams(), Matchers.hasKey("wait_for_completion"));
    }

    private static String extractMethodName(String apiName) {
        return apiName.substring(SUBMIT_TASK_PREFIX.length(), apiName.length() - SUBMIT_TASK_SUFFIX.length());
    }

    private static boolean isSubmitTaskMethod(String apiName) {
        return apiName.startsWith(SUBMIT_TASK_PREFIX) && apiName.endsWith(SUBMIT_TASK_SUFFIX);
    }

    private static Stream<Tuple<String, Method>> getSubClientMethods(String namespace, Class<?> clientClass) {
        return Arrays.stream(clientClass.getMethods()).filter(method -> method.getDeclaringClass().equals(clientClass))
                .map(method -> Tuple.tuple(namespace + "." + toSnakeCase(method.getName()), method))
                .flatMap(tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                    ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType()) : Stream.of(tuple));
    }

    private static String toSnakeCase(String camelCase) {
        StringBuilder snakeCaseString = new StringBuilder();
        for (Character aChar : camelCase.toCharArray()) {
            if (Character.isUpperCase(aChar)) {
                snakeCaseString.append('_');
                snakeCaseString.append(Character.toLowerCase(aChar));
            } else {
                snakeCaseString.append(aChar);
            }
        }
        return snakeCaseString.toString();
    }

    private static class TrackingActionListener implements ActionListener<Integer> {
        private final AtomicInteger statusCode = new AtomicInteger(-1);
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        @Override
        public void onResponse(Integer statusCode) {
            assertTrue(this.statusCode.compareAndSet(-1, statusCode));
        }

        @Override
        public void onFailure(Exception e) {
            assertTrue(exception.compareAndSet(null, e));
        }
    }

    private static StatusLine newStatusLine(RestStatus restStatus) {
        return new BasicStatusLine(HTTP_PROTOCOL, restStatus.getStatus(), restStatus.name());
    }
}
