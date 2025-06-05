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
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
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

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class VoyageAIActionCreatorTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
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
            sender.start();

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
                "secret",
                new VoyageAIEmbeddingsTaskSettings(null, true),
                1024,
                1024,
                "model",
                VoyageAIEmbeddingType.FLOAT
            );
            var actionCreator = new VoyageAIActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null);
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(
                new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), inputType),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            if (inputType != null && inputType != InputType.UNSPECIFIED) {
                var convertedInputType = convertToString(inputType);
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            "output_dtype",
                            "float",
                            "truncation",
                            true,
                            "input_type",
                            convertedInputType,
                            "output_dimension",
                            1024,
                            "input",
                            List.of("abc"),
                            "model",
                            "model"
                        )
                    )
                );
            } else {
                MatcherAssert.assertThat(
                    requestMap,
                    is(
                        Map.of(
                            "output_dtype",
                            "float",
                            "truncation",
                            true,
                            "output_dimension",
                            1024,
                            "input",
                            List.of("abc"),
                            "model",
                            "model"
                        )
                    )
                );
            }
        }
    }
}
