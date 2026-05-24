/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettingsTests;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.INPUT_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.INPUT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.OUTPUT_DIMENSION_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.OUTPUT_DTYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.TRUNCATION_FIELD;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.convertInputTypeToString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class VoyageAIActionCreatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String TEST_API_KEY = "secret";
    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String TEST_INPUT = "some input";
    private static final int TEST_DIMENSIONS = 1024;
    private static final int TEST_MAX_INPUT_TOKENS = 1024;
    private static final VoyageAIEmbeddingType TEST_EMBEDDING_TYPE = VoyageAIEmbeddingType.FLOAT;

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testCreate_VoyageAIEmbeddingsModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            String responseJson = """
                {
                    "object": "list",
                    "data": [{
                            "object": "embedding",
                            "embedding": [
                                0.123,
                                -0.123
                            ],
                            "index": 0
                    }],
                    "model": "voyage-3-large",
                    "usage": {
                        "total_tokens": 123
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = VoyageAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                TEST_API_KEY,
                new VoyageAIEmbeddingsTaskSettings(null, true),
                TEST_MAX_INPUT_TOKENS,
                TEST_DIMENSIONS,
                TEST_MODEL_ID,
                TEST_EMBEDDING_TYPE
            );
            var actionCreator = new VoyageAIActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null);
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(new EmbeddingsInput(List.of(TEST_INPUT), inputType), null, listener);

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                is(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + TEST_API_KEY));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertInputTypeToString(inputType);
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            OUTPUT_DTYPE_FIELD,
                            TEST_EMBEDDING_TYPE.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            INPUT_TYPE_FIELD,
                            convertedInputType,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS,
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID
                        )
                    )
                );
            } else {
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            OUTPUT_DTYPE_FIELD,
                            TEST_EMBEDDING_TYPE.toRequestString(),
                            TRUNCATION_FIELD,
                            true,
                            OUTPUT_DIMENSION_FIELD,
                            TEST_DIMENSIONS,
                            INPUT_FIELD,
                            List.of(TEST_INPUT),
                            MODEL_FIELD,
                            TEST_MODEL_ID
                        )
                    )
                );
            }
        }
    }
}
