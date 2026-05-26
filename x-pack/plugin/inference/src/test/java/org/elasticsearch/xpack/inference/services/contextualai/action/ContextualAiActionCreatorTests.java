/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModelTests;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettingsTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.RerankExpectation;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.buildExpectationRerank;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_QUERY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.INSTRUCTION_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.QUERY_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.TOP_N_FIELD;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class ContextualAiActionCreatorTests extends ESTestCase {

    private static final List<RerankExpectation> RERANK_EXPECTATIONS = List.of(
        new RerankExpectation(Map.of("index", 0, "relevance_score", 0.95f)),
        new RerankExpectation(Map.of("index", 1, "relevance_score", 0.72f))
    );

    private static final String TWO_RESULTS_RESPONSE_JSON = """
        {
            "results": [
                {
                    "index": 0,
                    "relevance_score": 0.95
                },
                {
                    "index": 1,
                    "relevance_score": 0.72
                }
            ]
        }
        """;

    private static final Settings NO_RETRY_SETTINGS = buildSettingsWithRetryFields(
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueSeconds(0)
    );

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

    public void testCreate_RerankAction_OnlyRequiredFields_Succeeds() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(TWO_RESULTS_RESPONSE_JSON));

            var model = ContextualAiRerankModelTests.createModel(getUrl(webServer), TEST_API_KEY, TEST_MODEL_ID, null, null);
            var actionCreator = new ContextualAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, Map.of());

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(new InferenceString(DataType.TEXT, TEST_QUERY), InferenceString.fromStringList(TEST_DOCUMENTS)),
                null,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS)));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().getFirst();
            assertThat(request.getUri().getQuery(), is(nullValue()));
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaTypeWithoutParameters()));
            assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + TEST_API_KEY));

            var requestMap = entityAsMap(request.getBody());
            // Validate that no unexpected fields are sent
            assertThat(requestMap, aMapWithSize(3));
            assertThat(requestMap.get(QUERY_FIELD), is(TEST_QUERY));
            assertThat(requestMap.get(DOCUMENTS_FIELD), is(TEST_DOCUMENTS));
            assertThat(requestMap.get(MODEL_FIELD), is(TEST_MODEL_ID));
        }
    }

    public void testCreate_RerankAction_WithTaskSettings_SendsExtraFields() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(TWO_RESULTS_RESPONSE_JSON));

            var model = ContextualAiRerankModelTests.createModel(
                getUrl(webServer),
                TEST_API_KEY,
                TEST_MODEL_ID,
                TEST_TOP_N,
                TEST_INSTRUCTION
            );
            var actionCreator = new ContextualAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, new HashMap<>());

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(new InferenceString(DataType.TEXT, TEST_QUERY), InferenceString.fromStringList(TEST_DOCUMENTS)),
                null,
                listener
            );

            listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            // Validate that all expected fields are sent (including those from task settings) and no unexpected fields are sent
            assertThat(requestMap, aMapWithSize(5));
            assertThat(requestMap.get(QUERY_FIELD), is(TEST_QUERY));
            assertThat(requestMap.get(DOCUMENTS_FIELD), is(TEST_DOCUMENTS));
            assertThat(requestMap.get(MODEL_FIELD), is(TEST_MODEL_ID));
            assertThat(requestMap.get(TOP_N_FIELD), is(TEST_TOP_N));
            assertThat(requestMap.get(INSTRUCTION_FIELD), is(TEST_INSTRUCTION));
        }
    }

    public void testCreate_RerankAction_WithOverriddenTaskSettings_OverridesTakeEffect() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(TWO_RESULTS_RESPONSE_JSON));

            var model = ContextualAiRerankModelTests.createModel(
                getUrl(webServer),
                TEST_API_KEY,
                TEST_MODEL_ID,
                INITIAL_TEST_TOP_N,
                INITIAL_TEST_INSTRUCTION
            );
            var actionCreator = new ContextualAiActionCreator(sender, createWithEmptySettings(threadPool));
            var taskSettingsOverride = ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(null, NEW_TEST_TOP_N, NEW_TEST_INSTRUCTION);
            var action = actionCreator.create(model, taskSettingsOverride);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(new InferenceString(DataType.TEXT, TEST_QUERY), InferenceString.fromStringList(TEST_DOCUMENTS)),
                null,
                listener
            );

            listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            // Validate that no unexpected fields are sent
            assertThat(requestMap, aMapWithSize(5));
            assertThat(requestMap.get(QUERY_FIELD), is(TEST_QUERY));
            assertThat(requestMap.get(DOCUMENTS_FIELD), is(TEST_DOCUMENTS));
            assertThat(requestMap.get(MODEL_FIELD), is(TEST_MODEL_ID));
            assertThat(requestMap.get(TOP_N_FIELD), is(NEW_TEST_TOP_N));
            assertThat(requestMap.get(INSTRUCTION_FIELD), is(NEW_TEST_INSTRUCTION));
        }
    }

    public void testCreate_RerankAction_WithRequestTopN_TakesPriorityOverModelTaskSettings() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(TWO_RESULTS_RESPONSE_JSON));

            var model = ContextualAiRerankModelTests.createModel(getUrl(webServer), TEST_API_KEY, TEST_MODEL_ID, INITIAL_TEST_TOP_N, null);
            var actionCreator = new ContextualAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, Map.of());

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(
                    new InferenceString(DataType.TEXT, TEST_QUERY),
                    InferenceString.fromStringList(TEST_DOCUMENTS),
                    null,
                    NEW_TEST_TOP_N,
                    false
                ),
                null,
                listener
            );

            listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            // Validate that no unexpected fields are sent
            assertThat(requestMap, aMapWithSize(4));
            assertThat(requestMap.get(QUERY_FIELD), is(TEST_QUERY));
            assertThat(requestMap.get(DOCUMENTS_FIELD), is(TEST_DOCUMENTS));
            assertThat(requestMap.get(MODEL_FIELD), is(TEST_MODEL_ID));
            assertThat(requestMap.get(TOP_N_FIELD), is(NEW_TEST_TOP_N));
        }
    }

    public void testCreate_RerankAction_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            String responseJson = """
                {
                    "not_results": [
                        {
                            "index": 0,
                            "relevance_score": 0.95
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ContextualAiRerankModelTests.createModel(getUrl(webServer), TEST_API_KEY, TEST_MODEL_ID, null, null);
            var action = new ContextualAiActionCreator(sender, createWithEmptySettings(threadPool)).create(model, Map.of());

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(new InferenceString(DataType.TEXT, TEST_QUERY), InferenceString.fromStringList(TEST_DOCUMENTS)),
                null,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(Strings.format("Failed to send ContextualAI rerank request. Cause: Required [results]", TEST_INFERENCE_ENTITY_ID))
            );
            assertThat(thrownException.getCause().getMessage(), is("Required [results]"));
        }
    }
}
