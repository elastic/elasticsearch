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
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.client.RestHighLevelClientTests.HTTP_PROTOCOL;
import static org.elasticsearch.client.RestHighLevelClientTests.newStatusLine;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoreClientTests extends ESTestCase {

    private static final RequestLine REQUEST_LINE = new BasicRequestLine(HttpGet.METHOD_NAME, "/", HTTP_PROTOCOL);

    private RestClient restClient;
    private CoreClient coreClient;

    @Before
    public void initClient() {
        restClient = mock(RestClient.class);
        coreClient = new CoreClient(restClient, Collections.emptyList());
    }
    
    @After
    public void closeClient() throws IOException {
        restClient.close();
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
                () -> coreClient.performRequest(request, null, RequestOptions.DEFAULT, null, null));
            assertSame(validationException, actualException);
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            coreClient.performRequestAsync(request, null, RequestOptions.DEFAULT, null, trackingActionListener, null);
            assertSame(validationException, trackingActionListener.exception.get());
        }
    }

    public void testParseEntity() throws IOException {
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> coreClient.parseEntity(null, null));
            assertEquals("Response body expected but not returned", ise.getMessage());
        }
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class,
                () -> coreClient.parseEntity(new StringEntity("", (ContentType) null), null));
            assertEquals("Elasticsearch didn't return the [Content-Type] header, unable to parse response body", ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("", ContentType.APPLICATION_SVG_XML);
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> coreClient.parseEntity(entity, null));
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
            assertEquals("value", coreClient.parseEntity(jsonEntity, entityParser));
            HttpEntity yamlEntity = new StringEntity("---\nfield: value\n", ContentType.create("application/yaml"));
            assertEquals("value", coreClient.parseEntity(yamlEntity, entityParser));
            HttpEntity smileEntity = createBinaryEntity(SmileXContent.contentBuilder(), ContentType.create("application/smile"));
            assertEquals("value", coreClient.parseEntity(smileEntity, entityParser));
            HttpEntity cborEntity = createBinaryEntity(CborXContent.contentBuilder(), ContentType.create("application/cbor"));
            assertEquals("value", coreClient.parseEntity(cborEntity, entityParser));
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
        boolean result = CoreClient.convertExistsResponse(response);
        assertEquals(restStatus == RestStatus.OK, result);
    }

    public void testParseResponseException() throws IOException {
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = coreClient.parseResponseException(responseException);
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
            ElasticsearchException elasticsearchException = coreClient.parseResponseException(responseException);
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
            ElasticsearchException elasticsearchException = coreClient.parseResponseException(responseException);
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
            ElasticsearchException elasticsearchException = coreClient.parseResponseException(responseException);
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
            Integer result = coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(), Collections.emptySet());
            assertEquals(restStatus.getStatus(), result.intValue());
        }
        {
            IOException ioe = expectThrows(IOException.class, () -> coreClient.performRequest(mainRequest,
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
            () -> coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
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
            () -> coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
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
            () -> coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
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
            () -> coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
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
        assertEquals(Integer.valueOf(404), coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
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
            () -> coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
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
            () -> coreClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT,
                response -> {throw new IllegalStateException();}, Collections.singleton(404)));
        assertEquals(RestStatus.NOT_FOUND, elasticsearchException.status());
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
    }

    public void testWrapResponseListenerOnSuccess() {
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = coreClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(newStatusLine(restStatus));
            responseListener.onSuccess(new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse));
            assertNull(trackingActionListener.exception.get());
            assertEquals(restStatus.getStatus(), trackingActionListener.statusCode.get());
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = coreClient.wrapResponseListener(
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
        ResponseListener responseListener = coreClient.wrapResponseListener(
            response -> response.getStatusLine().getStatusCode(), trackingActionListener, Collections.emptySet());
        IllegalStateException exception = new IllegalStateException();
        responseListener.onFailure(exception);
        assertSame(exception, trackingActionListener.exception.get());
    }

    public void testWrapResponseListenerOnResponseExceptionWithoutEntity() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = coreClient.wrapResponseListener(
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
        ResponseListener responseListener = coreClient.wrapResponseListener(
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
            ResponseListener responseListener = coreClient.wrapResponseListener(
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
            ResponseListener responseListener = coreClient.wrapResponseListener(
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
        ResponseListener responseListener = coreClient.wrapResponseListener(
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
        ResponseListener responseListener = coreClient.wrapResponseListener(
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
        ResponseListener responseListener = coreClient.wrapResponseListener(
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
        List<NamedXContentRegistry.Entry> namedXContents = CoreClient.getDefaultNamedXContents();
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
        List<NamedXContentRegistry.Entry> namedXContents = CoreClient.getProvidedNamedXContents();
        assertEquals(10, namedXContents.size());
        Map<Class<?>, Integer> categories = new HashMap<>();
        List<String> names = new ArrayList<>();
        for (NamedXContentRegistry.Entry namedXContent : namedXContents) {
            names.add(namedXContent.name.getPreferredName());
            Integer counter = categories.putIfAbsent(namedXContent.categoryClass, 1);
            if (counter != null) {
                categories.put(namedXContent.categoryClass, counter + 1);
            }
        }
        assertEquals(3, categories.size());
        assertEquals(Integer.valueOf(2), categories.get(Aggregation.class));
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
    }

    private static class TrackingActionListener implements ActionListener<Integer> {
        final AtomicInteger statusCode = new AtomicInteger(-1);
        final AtomicReference<Exception> exception = new AtomicReference<>();

        @Override
        public void onResponse(Integer statusCode) {
            assertTrue(this.statusCode.compareAndSet(-1, statusCode));
        }

        @Override
        public void onFailure(Exception e) {
            assertTrue(exception.compareAndSet(null, e));
        }
    }
}
