/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIRerankRequestEntity;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIUtils;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestEntity.INPUT_TEXT_FIELD;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JinaAIActionCreatorTests extends ESTestCase {
    private static final String TEST_API_KEY = "secret";
    private static final String TEST_EMBEDDING_MODEL_ID = "jina-embeddings-v3";
    private static final String TEST_RERANK_MODEL_ID = "jina-reranker-v2-base-multilingual";
    private static final String TEST_EMBEDDING_INPUT = "abc";
    private static final String TEST_QUERY = "query";
    private static final String TEST_DOCUMENT = "doc";

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

    public void testExecute_ReturnsSuccessfulResponse_ForTextEmbeddingAction() throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingAction(TaskType.TEXT_EMBEDDING);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingAction() throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingAction(TaskType.EMBEDDING);
    }

    private void testExecute_ReturnsSuccessfulResponse_ForEmbeddingAction(TaskType taskType) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "jina-embeddings-v3",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                TEST_EMBEDDING_MODEL_ID,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                TEST_API_KEY,
                taskType
            );
            var actionCreator = new JinaAIActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, Map.of());

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(List.of(TEST_EMBEDDING_INPUT), null), null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            if (taskType == TaskType.TEXT_EMBEDDING) {
                assertThat(
                    result.asMap(),
                    is(DenseEmbeddingFloatResultsTests.buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F })))
                );
            } else {
                assertThat(
                    result.asMap(),
                    is(GenericDenseEmbeddingFloatResultsTests.buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F })))
                );
            }
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + TEST_API_KEY));
            assertThat(webServer.requests().get(0).getHeader(JinaAIUtils.REQUEST_SOURCE_HEADER), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            if (taskType == TaskType.TEXT_EMBEDDING) {
                assertThat(requestMap.get(JinaAIEmbeddingsRequestEntity.INPUT_FIELD), is(List.of(TEST_EMBEDDING_INPUT)));
            } else {
                assertThat(
                    requestMap.get(JinaAIEmbeddingsRequestEntity.INPUT_FIELD),
                    is(List.of(Map.of(INPUT_TEXT_FIELD, TEST_EMBEDDING_INPUT)))
                );
            }
            assertThat(requestMap.get(JinaAIEmbeddingsRequestEntity.MODEL_FIELD), is(TEST_EMBEDDING_MODEL_ID));
            assertThat(requestMap.get(JinaAIEmbeddingsRequestEntity.EMBEDDING_TYPE_FIELD), is(JinaAIEmbeddingType.FLOAT.toRequestString()));
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForRerankAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "model": "jina-reranker-v2-base-multilingual",
                    "results": [
                        {
                            "index": 0,
                            "relevance_score": 0.94
                        }
                    ],
                    "usage": {
                        "total_tokens": 15
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), TEST_API_KEY, TEST_RERANK_MODEL_ID, null, null);
            var actionCreator = new JinaAIActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, Map.of());

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new QueryAndDocsInputs(TEST_QUERY, List.of(TEST_DOCUMENT), null, null, false), null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(
                result.asMap(),
                is(
                    RankedDocsResultsTests.buildExpectationRerank(
                        List.of(
                            new RankedDocsResultsTests.RerankExpectation(
                                Map.of(RankedDocsResults.RankedDoc.INDEX, 0, RankedDocsResults.RankedDoc.RELEVANCE_SCORE, 0.94f)
                            )
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + TEST_API_KEY));
            assertThat(webServer.requests().get(0).getHeader(JinaAIUtils.REQUEST_SOURCE_HEADER), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get(JinaAIRerankRequestEntity.MODEL_FIELD), is(TEST_RERANK_MODEL_ID));
            assertThat(requestMap.get(JinaAIRerankRequestEntity.QUERY_FIELD), is(TEST_QUERY));
            assertThat(requestMap.get(JinaAIRerankRequestEntity.DOCUMENTS_FIELD), is(List.of(TEST_DOCUMENT)));
        }
    }
}
