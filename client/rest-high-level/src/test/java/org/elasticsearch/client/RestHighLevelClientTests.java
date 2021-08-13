/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.MainRequest;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.ilm.AllocateAction;
import org.elasticsearch.client.ilm.DeleteAction;
import org.elasticsearch.client.ilm.ForceMergeAction;
import org.elasticsearch.client.ilm.FreezeAction;
import org.elasticsearch.client.ilm.LifecycleAction;
import org.elasticsearch.client.ilm.ReadOnlyAction;
import org.elasticsearch.client.ilm.RolloverAction;
import org.elasticsearch.client.ilm.SearchableSnapshotAction;
import org.elasticsearch.client.ilm.SetPriorityAction;
import org.elasticsearch.client.ilm.ShrinkAction;
import org.elasticsearch.client.ilm.UnfollowAction;
import org.elasticsearch.client.ilm.WaitForSnapshotAction;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalysis;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AccuracyMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AucRocMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.ConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.HuberMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredLogarithmicErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.client.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.client.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.client.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.client.ml.inference.preprocessing.CustomWordEmbedding;
import org.elasticsearch.client.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.client.ml.inference.preprocessing.Multi;
import org.elasticsearch.client.ml.inference.preprocessing.NGram;
import org.elasticsearch.client.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.client.ml.inference.preprocessing.TargetMeanEncoding;
import org.elasticsearch.client.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.Exponent;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.LogisticRegression;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.client.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.client.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.client.transform.transforms.SyncConfig;
import org.elasticsearch.client.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.index.rankeval.DiscountedCumulativeGain;
import org.elasticsearch.index.rankeval.EvaluationMetric;
import org.elasticsearch.index.rankeval.ExpectedReciprocalRank;
import org.elasticsearch.index.rankeval.MeanReciprocalRank;
import org.elasticsearch.index.rankeval.MetricDetail;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RecallAtK;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.RequestMatcher;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
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

import static org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestHighLevelClientTests extends ESTestCase {

    private static final String SUBMIT_TASK_PREFIX = "submit_";
    private static final String SUBMIT_TASK_SUFFIX = "_task";
    private static final ProtocolVersion HTTP_PROTOCOL = new ProtocolVersion("http", 1, 1);
    private static final RequestLine REQUEST_LINE = new BasicRequestLine(HttpGet.METHOD_NAME, "/", HTTP_PROTOCOL);

    /**
     * These APIs do not use a Request object (because they don't have a body, or any request parameters).
     * The method naming/parameter assertions use this {@code Set} to determine which rules to apply.
     * (This is also used for async variants of these APIs when they exist)
     */
    private static final Set<String> APIS_WITHOUT_REQUEST_OBJECT = Sets.newHashSet(
        // core
        "ping", "info",
        // security
        "security.get_ssl_certificates", "security.authenticate", "security.get_user_privileges", "security.get_builtin_privileges",
        // license
        "license.get_trial_status", "license.get_basic_status"

    );

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() throws IOException {
        restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());
        mockGetRoot(restClient);
    }

    /**
     * Mock rest client to return a valid response to async GET with the current build "/"
     */
    static void mockGetRoot(RestClient restClient) throws IOException{
        Build build = new Build(
            Build.Flavor.DEFAULT, Build.CURRENT.type(), Build.CURRENT.hash(),
            Build.CURRENT.date(), false, Build.CURRENT.getQualifiedVersion()
        );

        mockGetRoot(restClient, build, true);
    }

    /**
     *  Mock rest client to return a valid response to async GET with a specific build version "/"
     */
    public static void mockGetRoot(RestClient restClient, Build build, boolean setProductHeader) throws IOException {
        org.elasticsearch.action.main.MainResponse mainResp = new org.elasticsearch.action.main.MainResponse(
            "node",
            Version.fromString(build.getQualifiedVersion().replace("-SNAPSHOT", "")),
            new ClusterName("cluster"),
            "uuid",
            build
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        mainResp.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        mockGetRoot(restClient, baos.toByteArray(), setProductHeader);
    }

    /**
     *  Mock rest client to return a valid response to async GET with an arbitrary binary payload "/"
     */
    public static void mockGetRoot(RestClient restClient, byte[] responseBody, boolean setProductHeader) throws IOException {
        NByteArrayEntity entity = new NByteArrayEntity(responseBody, ContentType.APPLICATION_JSON);
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.OK));
        when(response.getEntity()).thenReturn(entity);
        if (setProductHeader) {
            when(response.getHeader("X-Elastic-Product")).thenReturn("Elasticsearch");
        }

        when(restClient
            .performRequestAsync(argThat(new RequestMatcher("GET", "/")), any()))
            .thenAnswer(i -> {
                    ((ResponseListener)i.getArguments()[1]).onSuccess(response);
                    return Cancellable.NO_OP;
                }
            );
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
        MainResponse testInfo = new MainResponse("nodeName", new MainResponse.Version("number", "buildFlavor", "buildType", "buildHash",
            "buildDate", true, "luceneVersion", "minimumWireCompatibilityVersion", "minimumIndexCompatibilityVersion"),
            "clusterName", "clusterUuid", "You Know, for Search");
        mockResponse((ToXContentFragment) (builder, params) -> {
            // taken from the server side MainResponse
            builder.field("name", testInfo.getNodeName());
            builder.field("cluster_name", testInfo.getClusterName());
            builder.field("cluster_uuid", testInfo.getClusterUuid());
            builder.startObject("version")
                .field("number", testInfo.getVersion().getNumber())
                .field("build_flavor", testInfo.getVersion().getBuildFlavor())
                .field("build_type", testInfo.getVersion().getBuildType())
                .field("build_hash", testInfo.getVersion().getBuildHash())
                .field("build_date", testInfo.getVersion().getBuildDate())
                .field("build_snapshot", testInfo.getVersion().isSnapshot())
                .field("lucene_version", testInfo.getVersion().getLuceneVersion())
                .field("minimum_wire_compatibility_version", testInfo.getVersion().getMinimumWireCompatibilityVersion())
                .field("minimum_index_compatibility_version", testInfo.getVersion().getMinimumIndexCompatibilityVersion())
                .endObject();
            builder.field("tagline", testInfo.getTagline());
            return builder;
        });
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
                    () -> restHighLevelClient.parseEntity(new NStringEntity("", (ContentType) null), null));
            assertEquals("Elasticsearch didn't return the [Content-Type] header, unable to parse response body", ise.getMessage());
        }
        {
            NStringEntity entity = new NStringEntity("", ContentType.APPLICATION_SVG_XML);
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
            HttpEntity jsonEntity = new NStringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON);
            assertEquals("value", restHighLevelClient.parseEntity(jsonEntity, entityParser));
            HttpEntity yamlEntity = new NStringEntity("---\nfield: value\n", ContentType.create("application/yaml"));
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
            return new NByteArrayEntity(BytesReference.bytes(builder).toBytesRef().bytes, contentType);
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
            httpResponse.setEntity(new NStringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
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
            httpResponse.setEntity(new NStringEntity("{\"error\":", ContentType.APPLICATION_JSON));
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
            httpResponse.setEntity(new NStringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
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
        httpResponse.setEntity(new NStringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
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
        httpResponse.setEntity(new NStringEntity("{\"error\":", ContentType.APPLICATION_JSON));
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
        httpResponse.setEntity(new NStringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
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
        httpResponse.setEntity(new NStringEntity("{\"error\":\"test error message\",\"status\":404}",
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
        httpResponse.setEntity(new NStringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
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
            httpResponse.setEntity(new NStringEntity("{\"error\":", ContentType.APPLICATION_JSON));
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
            httpResponse.setEntity(new NStringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
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
        httpResponse.setEntity(new NStringEntity("{\"error\":\"test error message\",\"status\":404}",
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

        // Explicitly check for metrics from the analytics module because they aren't in InternalAggregationTestCase
        assertTrue(namedXContents.removeIf(e -> e.name.getPreferredName().equals("string_stats")));
        assertTrue(namedXContents.removeIf(e -> e.name.getPreferredName().equals("top_metrics")));
        assertTrue(namedXContents.removeIf(e -> e.name.getPreferredName().equals("inference")));

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
        assertEquals(78, namedXContents.size());
        Map<Class<?>, Integer> categories = new HashMap<>();
        List<String> names = new ArrayList<>();
        for (NamedXContentRegistry.Entry namedXContent : namedXContents) {
            names.add(namedXContent.name.getPreferredName());
            Integer counter = categories.putIfAbsent(namedXContent.categoryClass, 1);
            if (counter != null) {
                categories.put(namedXContent.categoryClass, counter + 1);
            }
        }
        assertEquals("Had: " + categories, 16, categories.size());
        assertEquals(Integer.valueOf(3), categories.get(Aggregation.class));
        assertTrue(names.contains(ChildrenAggregationBuilder.NAME));
        assertTrue(names.contains(MatrixStatsAggregationBuilder.NAME));
        assertEquals(Integer.valueOf(5), categories.get(EvaluationMetric.class));
        assertTrue(names.contains(PrecisionAtK.NAME));
        assertTrue(names.contains(RecallAtK.NAME));
        assertTrue(names.contains(DiscountedCumulativeGain.NAME));
        assertTrue(names.contains(MeanReciprocalRank.NAME));
        assertTrue(names.contains(ExpectedReciprocalRank.NAME));
        assertEquals(Integer.valueOf(5), categories.get(MetricDetail.class));
        assertTrue(names.contains(PrecisionAtK.NAME));
        assertTrue(names.contains(RecallAtK.NAME));
        assertTrue(names.contains(MeanReciprocalRank.NAME));
        assertTrue(names.contains(DiscountedCumulativeGain.NAME));
        assertTrue(names.contains(ExpectedReciprocalRank.NAME));
        assertEquals(Integer.valueOf(12), categories.get(LifecycleAction.class));
        assertTrue(names.contains(UnfollowAction.NAME));
        assertTrue(names.contains(AllocateAction.NAME));
        assertTrue(names.contains(DeleteAction.NAME));
        assertTrue(names.contains(ForceMergeAction.NAME));
        assertTrue(names.contains(ReadOnlyAction.NAME));
        assertTrue(names.contains(RolloverAction.NAME));
        assertTrue(names.contains(WaitForSnapshotAction.NAME));
        assertTrue(names.contains(ShrinkAction.NAME));
        assertTrue(names.contains(FreezeAction.NAME));
        assertTrue(names.contains(SetPriorityAction.NAME));
        assertTrue(names.contains(SearchableSnapshotAction.NAME));
        assertEquals(Integer.valueOf(3), categories.get(DataFrameAnalysis.class));
        assertTrue(names.contains(org.elasticsearch.client.ml.dataframe.OutlierDetection.NAME.getPreferredName()));
        assertTrue(names.contains(org.elasticsearch.client.ml.dataframe.Regression.NAME.getPreferredName()));
        assertTrue(names.contains(org.elasticsearch.client.ml.dataframe.Classification.NAME.getPreferredName()));
        assertTrue(names.contains(OutlierDetectionStats.NAME.getPreferredName()));
        assertTrue(names.contains(RegressionStats.NAME.getPreferredName()));
        assertTrue(names.contains(ClassificationStats.NAME.getPreferredName()));
        assertEquals(Integer.valueOf(1), categories.get(SyncConfig.class));
        assertTrue(names.contains(TimeSyncConfig.NAME));
        assertEquals(Integer.valueOf(1), categories.get(RetentionPolicyConfig.class));
        assertTrue(names.contains(TimeRetentionPolicyConfig.NAME));
        assertEquals(Integer.valueOf(3), categories.get(org.elasticsearch.client.ml.dataframe.evaluation.Evaluation.class));
        assertThat(names, hasItems(OutlierDetection.NAME, Classification.NAME, Regression.NAME));
        assertEquals(Integer.valueOf(13), categories.get(org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric.class));
        assertThat(names,
            hasItems(
                registeredMetricName(OutlierDetection.NAME, AucRocMetric.NAME),
                registeredMetricName(OutlierDetection.NAME, PrecisionMetric.NAME),
                registeredMetricName(OutlierDetection.NAME, RecallMetric.NAME),
                registeredMetricName(OutlierDetection.NAME, ConfusionMatrixMetric.NAME),
                registeredMetricName(Classification.NAME, AucRocMetric.NAME),
                registeredMetricName(Classification.NAME, AccuracyMetric.NAME),
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.NAME),
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.NAME),
                registeredMetricName(Classification.NAME, MulticlassConfusionMatrixMetric.NAME),
                registeredMetricName(Regression.NAME, MeanSquaredErrorMetric.NAME),
                registeredMetricName(Regression.NAME, MeanSquaredLogarithmicErrorMetric.NAME),
                registeredMetricName(Regression.NAME, HuberMetric.NAME),
                registeredMetricName(Regression.NAME, RSquaredMetric.NAME)));
        assertEquals(Integer.valueOf(13), categories.get(org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric.Result.class));
        assertThat(names,
            hasItems(
                registeredMetricName(OutlierDetection.NAME, AucRocMetric.NAME),
                registeredMetricName(OutlierDetection.NAME, PrecisionMetric.NAME),
                registeredMetricName(OutlierDetection.NAME, RecallMetric.NAME),
                registeredMetricName(OutlierDetection.NAME, ConfusionMatrixMetric.NAME),
                registeredMetricName(Classification.NAME, AucRocMetric.NAME),
                registeredMetricName(Classification.NAME, AccuracyMetric.NAME),
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.NAME),
                registeredMetricName(
                    Classification.NAME, org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.NAME),
                registeredMetricName(Classification.NAME, MulticlassConfusionMatrixMetric.NAME),
                registeredMetricName(Regression.NAME, MeanSquaredErrorMetric.NAME),
                registeredMetricName(Regression.NAME, MeanSquaredLogarithmicErrorMetric.NAME),
                registeredMetricName(Regression.NAME, HuberMetric.NAME),
                registeredMetricName(Regression.NAME, RSquaredMetric.NAME)));
        assertEquals(Integer.valueOf(6), categories.get(org.elasticsearch.client.ml.inference.preprocessing.PreProcessor.class));
        assertThat(names,
            hasItems(
                FrequencyEncoding.NAME,
                OneHotEncoding.NAME,
                TargetMeanEncoding.NAME,
                CustomWordEmbedding.NAME,
                NGram.NAME,
                Multi.NAME
            ));
        assertEquals(Integer.valueOf(3), categories.get(org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel.class));
        assertThat(names, hasItems(Tree.NAME, Ensemble.NAME, LangIdentNeuralNetwork.NAME));
        assertEquals(Integer.valueOf(4),
            categories.get(org.elasticsearch.client.ml.inference.trainedmodel.ensemble.OutputAggregator.class));
        assertThat(names, hasItems(WeightedMode.NAME, WeightedSum.NAME, LogisticRegression.NAME, Exponent.NAME));
        assertEquals(Integer.valueOf(2),
            categories.get(org.elasticsearch.client.ml.inference.trainedmodel.InferenceConfig.class));
        assertThat(names, hasItems(ClassificationConfig.NAME.getPreferredName(), RegressionConfig.NAME.getPreferredName()));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/70041")
    public void testApiNamingConventions() throws Exception {
        //this list should be empty once the high-level client is feature complete
        String[] notYetSupportedApi = new String[]{
            "create",
            "get_script_context",
            "get_script_languages",
            "indices.exists_type",
            "indices.put_alias",
            "render_search_template",
            "scripts_painless_execute",
            "indices.simulate_template",
            "indices.resolve_index",
            "indices.add_block",
            "open_point_in_time",
            "close_point_in_time"
        };
        //These API are not required for high-level client feature completeness
        String[] notRequiredApi = new String[] {
            "cluster.allocation_explain",
            "cluster.pending_tasks",
            "cluster.reroute",
            "cluster.state",
            "cluster.stats",
            "cluster.post_voting_config_exclusions",
            "cluster.delete_voting_config_exclusions",
            "dangling_indices.delete_dangling_index",
            "dangling_indices.import_dangling_index",
            "dangling_indices.list_dangling_indices",
            "indices.shard_stores",
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
        List<String> booleanReturnMethods = Arrays.asList(
            "security.enable_user",
            "security.disable_user",
            "security.change_password");
        Set<String> deprecatedMethods = new HashSet<>();
        deprecatedMethods.add("indices.force_merge");
        deprecatedMethods.add("multi_get");
        deprecatedMethods.add("multi_search");
        deprecatedMethods.add("search_scroll");

        ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load("rest-api-spec/api");
        Set<String> apiSpec = restSpec.getApis().stream().map(ClientYamlSuiteRestApi::getName).collect(Collectors.toSet());
        Set<String> apiUnsupported = new HashSet<>(apiSpec);
        Set<String> apiNotFound = new HashSet<>();

        Set<String> topLevelMethodsExclusions = new HashSet<>();
        topLevelMethodsExclusions.add("getLowLevelClient");
        topLevelMethodsExclusions.add("close");

        Map<String, Set<Method>> methods = Arrays.stream(RestHighLevelClient.class.getMethods())
                .filter(method -> method.getDeclaringClass().equals(RestHighLevelClient.class)
                        && topLevelMethodsExclusions.contains(method.getName()) == false)
                .map(method -> Tuple.tuple(toSnakeCase(method.getName()), method))
                .flatMap(tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                        ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType()) : Stream.of(tuple))
                .filter(tuple -> tuple.v2().getAnnotation(Deprecated.class) == null)
                .collect(Collectors.groupingBy(Tuple::v1,
                    Collectors.mapping(Tuple::v2, Collectors.toSet())));

        // TODO remove in 8.0 - we will undeprecate indices.get_template because the current getIndexTemplate
        // impl will replace the existing getTemplate method.
        // The above general-purpose code ignores all deprecated methods which in this case leaves `getTemplate`
        // looking like it doesn't have a valid implementatation when it does.
        apiUnsupported.remove("indices.get_template");

        // Synced flush is deprecated
        apiUnsupported.remove("indices.flush_synced");

        for (Map.Entry<String, Set<Method>> entry : methods.entrySet()) {
            String apiName = entry.getKey();

            for (Method method : entry.getValue()) {
                assertTrue("method [" + apiName + "] is not final",
                    Modifier.isFinal(method.getClass().getModifiers()) || Modifier.isFinal(method.getModifiers()));
                assertTrue("method [" + method + "] should be public", Modifier.isPublic(method.getModifiers()));

                //we convert all the method names to snake case, hence we need to look for the '_async' suffix rather than 'Async'
                if (apiName.endsWith("_async")) {
                    assertAsyncMethod(methods, method, apiName);
                } else if (isSubmitTaskMethod(apiName)) {
                    assertSubmitTaskMethod(methods, method, apiName, restSpec);
                } else {
                    assertSyncMethod(method, apiName, booleanReturnMethods);
                    apiUnsupported.remove(apiName);
                    if (apiSpec.contains(apiName) == false) {
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
                                apiName.startsWith("enrich.") == false &&
                                apiName.startsWith("transform.") == false &&
                                apiName.startsWith("text_structure.") == false &&
                                apiName.startsWith("searchable_snapshots.") == false &&
                                apiName.startsWith("eql.") == false &&
                                apiName.endsWith("freeze") == false &&
                                apiName.endsWith("reload_analyzers") == false &&
                                apiName.startsWith("async_search") == false &&
                                // IndicesClientIT.getIndexTemplate should be renamed "getTemplate" in version 8.0 when we
                                // can get rid of 7.0's deprecated "getTemplate"
                                apiName.equals("indices.get_index_template") == false &&
                                List.of("indices.data_streams_stats",
                                    "indices.delete_data_stream",
                                    "indices.create_data_stream",
                                    "indices.get_data_stream").contains(apiName) == false) {
                                apiNotFound.add(apiName);
                            }
                        }
                    }
                }
            }
        }
        assertThat("Some client method doesn't match a corresponding API defined in the REST spec: " + apiNotFound,
            apiNotFound.size(), equalTo(0));

        //we decided not to support cat API in the high-level REST client, they are supposed to be used from a low-level client
        apiUnsupported.removeIf(api -> api.startsWith("cat."));
        Stream.concat(Arrays.stream(notYetSupportedApi), Arrays.stream(notRequiredApi)).forEach(
            api -> assertTrue(api + " API is either not defined in the spec or already supported by the high-level client",
                apiUnsupported.remove(api)));
        assertThat("Some API are not supported but they should be: " + apiUnsupported, apiUnsupported.size(), equalTo(0));
    }

    private static void doTestProductCompatibilityCheck(
        boolean shouldBeAccepted, String version, boolean setProductHeader) throws Exception {

        // An endpoint different from "/" that returns a boolean
        GetSourceRequest apiRequest = new GetSourceRequest("foo", "bar");

        StatusLine apiStatus = mock(StatusLine.class);
        when(apiStatus.getStatusCode()).thenReturn(200);

        Response apiResponse = mock(Response.class);
        when(apiResponse.getStatusLine()).thenReturn(apiStatus);

        RestClient restClient = mock(RestClient.class);

        Build build = new Build(Build.Flavor.DEFAULT, Build.Type.UNKNOWN, "hash", "date", false, version);
        mockGetRoot(restClient, build, setProductHeader);
        when(restClient.performRequest(argThat(new RequestMatcher("HEAD", "/foo/_source/bar")))).thenReturn(apiResponse);

        RestHighLevelClient highLevelClient =  new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());

        if (shouldBeAccepted) {
            assertTrue(highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT));
        } else {
            expectThrows(ElasticsearchException.class, () ->
                highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT)
            );
        }
    }

    public void testProductCompatibilityCheck() throws Exception {
        // Version < 6.0.0
        doTestProductCompatibilityCheck(false, "5.0.0", false);

        // Version < 6.0.0, product header
        doTestProductCompatibilityCheck(false, "5.0.0", true);

        // Version 6.x -
        doTestProductCompatibilityCheck(true, "6.0.0", false);

        // Version 7.x, x < 14
        doTestProductCompatibilityCheck(true, "7.0.0", false);

        // Version 7.14, no product header
        doTestProductCompatibilityCheck(false, "7.14.0", false);

        // Version 7.14, product header
        doTestProductCompatibilityCheck(true, "7.14.0", true);

        // Version 8.x, no product header
        doTestProductCompatibilityCheck(false, "8.0.0", false);

        // Version 8.x, product header
        doTestProductCompatibilityCheck(true, "8.0.0", true);
    }

    public void testProductCompatibilityTagline() throws Exception {

        // An endpoint different from "/" that returns a boolean
        GetSourceRequest apiRequest = new GetSourceRequest("foo", "bar");
        StatusLine apiStatus = mock(StatusLine.class);
        when(apiStatus.getStatusCode()).thenReturn(200);
        Response apiResponse = mock(Response.class);
        when(apiResponse.getStatusLine()).thenReturn(apiStatus);
        when(restClient.performRequest(argThat(new RequestMatcher("HEAD", "/foo/_source/bar")))).thenReturn(apiResponse);

        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());

        byte[] bytes = ("{" +
            "  'cluster_name': '97b2b946a8494276822c3876d78d4f9c', " +
            "  'cluster_uuid': 'SUXRYY1fQ5uMKEiykuR5ZA', " +
            "  'version': { " +
            "    'build_date': '2021-03-18T06:17:15.410153305Z', " +
            "    'minimum_wire_compatibility_version': '6.8.0', " +
            "    'build_hash': '78722783c38caa25a70982b5b042074cde5d3b3a', " +
            "    'number': '7.12.0', " +
            "    'lucene_version': '8.8.0', " +
            "    'minimum_index_compatibility_version': '6.0.0-beta1', " +
            "    'build_flavor': 'default', " +
            "    'build_snapshot': false, " +
            "    'build_type': 'docker' " +
            "  }, " +
            "  'name': 'instance-0000000000', " +
            "  'tagline': 'hello world'" +
            "}"
        ).replace('\'', '"').getBytes(StandardCharsets.UTF_8);

        mockGetRoot(restClient, bytes, true);

        expectThrows(ElasticsearchException.class, () ->
            highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT)
        );
    }

    public void testProductCompatibilityFlavor() throws Exception {

        // An endpoint different from "/" that returns a boolean
        GetSourceRequest apiRequest = new GetSourceRequest("foo", "bar");
        StatusLine apiStatus = mock(StatusLine.class);
        when(apiStatus.getStatusCode()).thenReturn(200);
        Response apiResponse = mock(Response.class);
        when(apiResponse.getStatusLine()).thenReturn(apiStatus);
        when(restClient.performRequest(argThat(new RequestMatcher("HEAD", "/foo/_source/bar")))).thenReturn(apiResponse);

        RestHighLevelClient highLevelClient =  new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());

        byte[]
            bytes = ("{" +
            "  'cluster_name': '97b2b946a8494276822c3876d78d4f9c', " +
            "  'cluster_uuid': 'SUXRYY1fQ5uMKEiykuR5ZA', " +
            "  'version': { " +
            "    'build_date': '2021-03-18T06:17:15.410153305Z', " +
            "    'minimum_wire_compatibility_version': '6.8.0', " +
            "    'build_hash': '78722783c38caa25a70982b5b042074cde5d3b3a', " +
            "    'number': '7.12.0', " +
            "    'lucene_version': '8.8.0', " +
            "    'minimum_index_compatibility_version': '6.0.0-beta1', " +
            // Invalid flavor
            "    'build_flavor': 'foo', " +
            "    'build_snapshot': false, " +
            "    'build_type': 'docker' " +
            "  }, " +
            "  'name': 'instance-0000000000', " +
            "  'tagline': 'You Know, for Search'" +
            "}"
        ).replace('\'', '"').getBytes(StandardCharsets.UTF_8);

        mockGetRoot(restClient, bytes, true);

        expectThrows(ElasticsearchException.class, () ->
            highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT)
        );
    }

    public void testProductCompatibilityRequestFailure() throws Exception {

        RestClient restClient = mock(RestClient.class);

        // An endpoint different from "/" that returns a boolean
        GetSourceRequest apiRequest = new GetSourceRequest("foo", "bar");
        StatusLine apiStatus = mock(StatusLine.class);
        when(apiStatus.getStatusCode()).thenReturn(200);
        Response apiResponse = mock(Response.class);
        when(apiResponse.getStatusLine()).thenReturn(apiStatus);
        when(restClient.performRequest(argThat(new RequestMatcher("HEAD", "/foo/_source/bar")))).thenReturn(apiResponse);

        // Have the verification request fail
        when(restClient.performRequestAsync(argThat(new RequestMatcher("GET", "/")), any()))
            .thenAnswer(i -> {
                ((ResponseListener)i.getArguments()[1]).onFailure(new IOException("Something bad happened"));
                return Cancellable.NO_OP;
            });

        RestHighLevelClient highLevelClient =  new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());

        expectThrows(ElasticsearchException.class, () -> {
            highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT);
        });

        // Now have the validation request succeed
        Build build = new Build(Build.Flavor.DEFAULT, Build.Type.UNKNOWN, "hash", "date", false, "7.14.0");
        mockGetRoot(restClient, build, true);

        // API request should now succeed as validation has been retried
        assertTrue(highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT));
    }

    public void testProductCompatibilityWithForbiddenInfoEndpoint() throws Exception {
        RestClient restClient = mock(RestClient.class);

        // An endpoint different from "/" that returns a boolean
        GetSourceRequest apiRequest = new GetSourceRequest("foo", "bar");
        StatusLine apiStatus = mock(StatusLine.class);
        when(apiStatus.getStatusCode()).thenReturn(200);
        Response apiResponse = mock(Response.class);
        when(apiResponse.getStatusLine()).thenReturn(apiStatus);
        when(restClient.performRequest(argThat(new RequestMatcher("HEAD", "/foo/_source/bar")))).thenReturn(apiResponse);

        // Have the info endpoint used for verification return a 403 (forbidden)
        when(restClient.performRequestAsync(argThat(new RequestMatcher("GET", "/")), any()))
            .thenAnswer(i -> {
                StatusLine infoStatus = mock(StatusLine.class);
                when(apiStatus.getStatusCode()).thenReturn(HttpStatus.SC_FORBIDDEN);
                Response infoResponse = mock(Response.class);
                when(apiResponse.getStatusLine()).thenReturn(infoStatus);
                ((ResponseListener)i.getArguments()[1]).onSuccess(infoResponse);
                return Cancellable.NO_OP;
            });

        RestHighLevelClient highLevelClient =  new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());

        // API request should succeed
        Build build = new Build(Build.Flavor.DEFAULT, Build.Type.UNKNOWN, "hash", "date", false, "7.14.0");
        mockGetRoot(restClient, build, true);

        assertTrue(highLevelClient.existsSource(apiRequest, RequestOptions.DEFAULT));
    }

    public void testCancellationForwarding() throws Exception {

        mockGetRoot(restClient);
        Cancellable cancellable = mock(Cancellable.class);
        when(restClient.performRequestAsync(argThat(new RequestMatcher("HEAD", "/foo/_source/bar")), any())).thenReturn(cancellable);

        Cancellable result = restHighLevelClient.existsSourceAsync(
            new GetSourceRequest("foo", "bar"),
            RequestOptions.DEFAULT, ActionListener.wrap(() -> {})
        );

        result.cancel();
        verify(cancellable, times(1)).cancel();
    }

    private static void assertSyncMethod(Method method, String apiName, List<String> booleanReturnMethods) {
        //A few methods return a boolean rather than a response object
        if (apiName.equals("ping") || apiName.contains("exist") || booleanReturnMethods.contains(apiName)) {
            assertThat("the return type for method [" + method + "] is incorrect",
                method.getReturnType().getSimpleName(), equalTo("boolean"));
        } else {
            // It's acceptable for 404s to be represented as empty Optionals
            if (method.getReturnType().isAssignableFrom(Optional.class) == false) {
                assertThat("the return type for method [" + method + "] is incorrect",
                    method.getReturnType().getSimpleName(), endsWith("Response"));
            }
        }

        assertEquals("incorrect number of exceptions for method [" + method + "]", 1, method.getExceptionTypes().length);
        //a few methods don't accept a request object as argument
        if (APIS_WITHOUT_REQUEST_OBJECT.contains(apiName)) {
            assertEquals("incorrect number of arguments for method [" + method + "]", 1, method.getParameterTypes().length);
            assertThat("the parameter to method [" + method + "] is the wrong type",
                method.getParameterTypes()[0], equalTo(RequestOptions.class));
        } else {
            assertEquals("incorrect number of arguments for method [" + method + "]", 2, method.getParameterTypes().length);
            // This is no longer true for all methods. Some methods can contain these 2 args backwards because of deprecation
            if (method.getParameterTypes()[0].equals(RequestOptions.class)) {
                assertThat("the first parameter to method [" + method + "] is the wrong type",
                    method.getParameterTypes()[0], equalTo(RequestOptions.class));
                assertThat("the second parameter to method [" + method + "] is the wrong type",
                    method.getParameterTypes()[1].getSimpleName(), endsWith("Request"));
            } else {
                assertThat("the first parameter to method [" + method + "] is the wrong type",
                    method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
                assertThat("the second parameter to method [" + method + "] is the wrong type",
                    method.getParameterTypes()[1], equalTo(RequestOptions.class));
            }
        }
    }

    private static void assertAsyncMethod(Map<String, Set<Method>> methods, Method method, String apiName) {
        assertTrue("async method [" + method.getName() + "] doesn't have corresponding sync method",
                methods.containsKey(apiName.substring(0, apiName.length() - 6)));
        assertThat("async method [" + method + "] should return Cancellable", method.getReturnType(), equalTo(Cancellable.class));
        assertEquals("async method [" + method + "] should not throw any exceptions", 0, method.getExceptionTypes().length);
        if (APIS_WITHOUT_REQUEST_OBJECT.contains(apiName.replaceAll("_async$", ""))) {
            assertEquals(2, method.getParameterTypes().length);
            assertThat(method.getParameterTypes()[0], equalTo(RequestOptions.class));
            assertThat(method.getParameterTypes()[1], equalTo(ActionListener.class));
        } else {
            assertEquals("async method [" + method + "] has the wrong number of arguments", 3, method.getParameterTypes().length);
            // This is no longer true for all methods. Some methods can contain these 2 args backwards because of deprecation
            if (method.getParameterTypes()[0].equals(RequestOptions.class)) {
                assertThat("the first parameter to async method [" + method + "] should be a request type",
                    method.getParameterTypes()[0], equalTo(RequestOptions.class));
                assertThat("the second parameter to async method [" + method + "] is the wrong type",
                    method.getParameterTypes()[1].getSimpleName(), endsWith("Request"));
            } else {
                assertThat("the first parameter to async method [" + method + "] should be a request type",
                    method.getParameterTypes()[0].getSimpleName(), endsWith("Request"));
                assertThat("the second parameter to async method [" + method + "] is the wrong type",
                    method.getParameterTypes()[1], equalTo(RequestOptions.class));
            }
            assertThat("the third parameter to async method [" + method + "] is the wrong type",
                method.getParameterTypes()[2], equalTo(ActionListener.class));
        }
    }

    private static void assertSubmitTaskMethod(Map<String, Set<Method>> methods, Method method, String apiName,
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
