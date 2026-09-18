/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceConfigurationTests;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiRequestUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModelTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResults.RankedDoc.INDEX;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResults.RankedDoc.RELEVANCE_SCORE;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResults.RankedDoc.TEXT;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.buildExpectationRerank;
import static org.elasticsearch.xpack.inference.Utils.buildExpectationCompletions;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.INPUT_TYPE;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class OciGenAiServiceTests extends InferenceServiceTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String EMBEDDINGS_MODEL = "cohere.embed-v4.0";
    private static final String CHAT_MODEL = "meta.llama-3.3-70b-instruct";
    private static final String RERANK_MODEL = "cohere.rerank-v3.5";
    private static final Pattern AUTHORIZATION_PATTERN = Pattern.compile(
        "Signature version=\"1\",keyId=\"([^\"]+)\",algorithm=\"rsa-sha256\",headers=\"([^\"]+)\",signature=\"([^\"]+)\""
    );

    private static final String REGION_DESCRIPTION = "The OCI region identifier of the Generative AI inference endpoint, for "
        + "example us-chicago-1. Either the region or the url must be provided.";
    private static final String MODEL_ID_DESCRIPTION = "The OCI Generative AI model to use for "
        + "the inference task, for example cohere.embed-v4.0 or meta.llama-3.3-70b-instruct.";
    private static final String ENDPOINT_ID_DESCRIPTION = "The OCID of a dedicated AI cluster endpoint hosting "
        + "the model. When omitted the model is served on-demand.";
    private static final String URL_DESCRIPTION = "The base URL of the Generative AI inference endpoint, overriding the public "
        + "regional endpoint derived from the region (for example a private endpoint or a different OCI realm).";
    private static final String DIMENSIONS_DESCRIPTION = "The number of dimensions of the embeddings. Passed to the model "
        + "as the output dimensions when supported (cohere.embed-v4.0); otherwise discovered from the model.";
    private static final String PRIVATE_KEY_DESCRIPTION = "The PEM encoded RSA private key of the "
        + "API signing key (PKCS#8 or PKCS#1, not passphrase protected).";

    private static final String EMBEDDINGS_RESPONSE = """
        {
            "embeddings": [ [ 0.0123, -0.0123 ] ],
            "id": "abc",
            "modelId": "cohere.embed-v4.0",
            "modelVersion": "4.0",
            "usage": { "completionTokens": 0, "promptTokens": 4, "totalTokens": 4 }
        }
        """;

    private static final String CHAT_RESPONSE = """
        {
          "chatResponse": {
            "apiFormat": "GENERIC",
            "choices": [ {
              "finishReason": "stop",
              "index": 0,
              "message": { "content": [ { "text": "Hello, how are you today?", "type": "TEXT" } ], "role": "ASSISTANT", "toolCalls": [] }
            } ],
            "usage": { "completionTokens": 8, "promptTokens": 45, "totalTokens": 53 }
          },
          "modelId": "meta.llama-3.3-70b-instruct",
          "modelVersion": "1.0.0"
        }
        """;

    private static final String CHAT_STREAM_RESPONSE = """
        data: {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":""}]},"pad":"aaa"}

        data: {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":"Hello"}]},"pad":"aaaaa"}

        data: {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":", world"}]},"pad":"aa"}

        data: {"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":""}]},"finishReason":"stop","pad":"aaa"}

        data: [DONE]

        """;

    // ---------------------------------------------------------------------------------------------------------------------------
    // parsing
    // ---------------------------------------------------------------------------------------------------------------------------

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = createInferenceService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OciGenAiEmbeddingsModel.class));

                var embeddingsModel = (OciGenAiEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().region(), is(REGION_VALUE));
                assertThat(embeddingsModel.getServiceSettings().compartmentId(), is(COMPARTMENT_ID));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is(EMBEDDINGS_MODEL));
                assertFalse(embeddingsModel.getServiceSettings().dimensionsSetByUser());
                assertThat(embeddingsModel.getTaskSettings().getInputType(), is(InputType.INGEST));
                assertThat(embeddingsModel.getSecretSettings().keyId(), is(OciGenAiTestUtils.keyId()));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(
                    embeddingsModel.uri(),
                    is(URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/embedText"))
                );
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    OciGenAiTestUtils.serviceSettingsMap(EMBEDDINGS_MODEL),
                    new HashMap<>(Map.of(INPUT_TYPE, "ingest")),
                    createRandomChunkingSettingsMap(),
                    OciGenAiTestUtils.secretSettingsMap()
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesChatCompletionModels() throws IOException {
        for (var taskType : List.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)) {
            try (var service = createInferenceService()) {
                ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                    assertThat(model, instanceOf(OciGenAiChatCompletionModel.class));

                    var chatModel = (OciGenAiChatCompletionModel) model;
                    assertThat(chatModel.getTaskType(), is(taskType));
                    assertThat(chatModel.getServiceSettings().modelId(), is(CHAT_MODEL));
                    assertThat(chatModel.getServiceSettings().apiFormat(), is(OciGenAiChatApiFormat.GENERIC));
                    assertThat(chatModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
                    assertThat(
                        chatModel.uri(),
                        is(URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/chat"))
                    );
                }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

                service.parseRequestConfig(
                    "id",
                    taskType,
                    getRequestConfigMap(
                        OciGenAiTestUtils.serviceSettingsMap(CHAT_MODEL),
                        new HashMap<>(),
                        OciGenAiTestUtils.secretSettingsMap()
                    ),
                    modelListener
                );
            }
        }
    }

    public void testParseRequestConfig_CreatesARerankModel() throws IOException {
        try (var service = createInferenceService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OciGenAiRerankModel.class));

                var rerankModel = (OciGenAiRerankModel) model;
                assertThat(rerankModel.getServiceSettings().modelId(), is(RERANK_MODEL));
                assertThat(rerankModel.getTaskSettings().getTopN(), is(3));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.RERANK,
                getRequestConfigMap(
                    OciGenAiTestUtils.serviceSettingsMap(RERANK_MODEL),
                    new HashMap<>(Map.of("top_n", 3)),
                    OciGenAiTestUtils.secretSettingsMap()
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedTaskType() throws IOException {
        try (var service = createInferenceService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [ocigenai] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    OciGenAiTestUtils.serviceSettingsMap(EMBEDDINGS_MODEL),
                    new HashMap<>(),
                    OciGenAiTestUtils.secretSettingsMap()
                ),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettings = OciGenAiTestUtils.serviceSettingsMap(EMBEDDINGS_MODEL);
            serviceSettings.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [ocigenai] service"
            );

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(serviceSettings, new HashMap<>(), OciGenAiTestUtils.secretSettingsMap()),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenThePrivateKeyIsInvalid() throws IOException {
        try (var service = createInferenceService()) {
            var listener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    OciGenAiTestUtils.serviceSettingsMap(EMBEDDINGS_MODEL),
                    new HashMap<>(),
                    OciGenAiTestUtils.secretSettingsMap("not a pem")
                ),
                listener
            );

            var exception = expectThrows(ValidationException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("[service_settings] Invalid value for [private_key]"));
        }
    }

    public void testParsePersistedConfig_WithSecrets_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettings = OciGenAiTestUtils.serviceSettingsMap(EMBEDDINGS_MODEL);
            serviceSettings.put(ServiceFields.DIMENSIONS, 1536);
            serviceSettings.put(ServiceFields.DIMENSIONS_SET_BY_USER, false);
            serviceSettings.put("extra_key", "value");
            var secretSettings = OciGenAiTestUtils.secretSettingsMap();
            secretSettings.put("extra_key", "value");
            var persistedConfig = getPersistedConfigMap(serviceSettings, new HashMap<>(Map.of(INPUT_TYPE, "search")), secretSettings);
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig(
                new UnparsedModel("id", TaskType.TEXT_EMBEDDING, OciGenAiService.NAME, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(model, instanceOf(OciGenAiEmbeddingsModel.class));
            var embeddingsModel = (OciGenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(EMBEDDINGS_MODEL));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1536));
            assertFalse(embeddingsModel.getServiceSettings().dimensionsSetByUser());
            assertThat(embeddingsModel.getTaskSettings().getInputType(), is(InputType.SEARCH));
            assertThat(embeddingsModel.getSecretSettings().keyId(), is(OciGenAiTestUtils.keyId()));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testParsePersistedConfig_WithoutSecrets_CreatesAChatCompletionModel() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(OciGenAiTestUtils.serviceSettingsMap(CHAT_MODEL), new HashMap<>(), null);

            var model = service.parsePersistedConfig(
                new UnparsedModel("id", TaskType.CHAT_COMPLETION, OciGenAiService.NAME, persistedConfig.config(), null)
            );

            assertThat(model, instanceOf(OciGenAiChatCompletionModel.class));
            assertThat(model.getSecretSettings(), Matchers.nullValue());
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // inference
    // ---------------------------------------------------------------------------------------------------------------------------

    public void testInfer_SendsASignedEmbeddingsRequest() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OciGenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));

            var model = createSignedEmbeddingsModel(getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, List.of("input"), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().get(0);
            assertThat(request.getUri().getPath(), is("/20231130/actions/embedText"));
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(request.getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "inputs",
                        List.of("input"),
                        "compartmentId",
                        COMPARTMENT_ID,
                        "servingMode",
                        Map.of("servingType", "ON_DEMAND", "modelId", EMBEDDINGS_MODEL),
                        "inputType",
                        "SEARCH_DOCUMENT"
                    )
                )
            );

            // the request carries a valid OCI request signature computed over the headers the HTTP client actually sent
            var body = request.getBody().getBytes(StandardCharsets.UTF_8);
            assertThat(request.getHeader("x-content-sha256"), is(Base64.getEncoder().encodeToString(MessageDigests.sha256().digest(body))));
            assertThat(request.getHeader("Date"), notNullValue());
            assertThat(request.getHeader("Content-Length"), is(String.valueOf(body.length)));

            var matcher = AUTHORIZATION_PATTERN.matcher(request.getHeader(HttpHeaders.AUTHORIZATION));
            assertTrue("unexpected authorization header: " + request.getHeader(HttpHeaders.AUTHORIZATION), matcher.matches());
            assertThat(matcher.group(1), is(OciGenAiTestUtils.keyId()));
            assertThat(matcher.group(2), is("(request-target) host date x-content-sha256 content-type content-length"));

            var signingString = String.join(
                "\n",
                "(request-target): post /20231130/actions/embedText",
                "host: " + request.getHeader("Host"),
                "date: " + request.getHeader("Date"),
                "x-content-sha256: " + request.getHeader("x-content-sha256"),
                "content-type: " + request.getHeader(HttpHeaders.CONTENT_TYPE),
                "content-length: " + request.getHeader("Content-Length")
            );
            var signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(OciGenAiTestUtils.keyPair().getPublic());
            signature.update(signingString.getBytes(StandardCharsets.UTF_8));
            assertTrue(
                "signature does not verify against the sent headers",
                signature.verify(Base64.getDecoder().decode(matcher.group(3)))
            );
        }
    }

    public void testInfer_UnspecifiedInputType_DoesNotSendAnInputType() throws IOException {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));

            var model = OciGenAiEmbeddingsModelTests.createModel(getUrl(webServer), EMBEDDINGS_MODEL, null, null, null, null);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, List.of("input"), false, new HashMap<>(), InputType.UNSPECIFIED, null, listener);
            listener.actionGet(TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));
            assertFalse(entityAsMap(webServer.requests().get(0).getBody()).containsKey("inputType"));
        }
    }

    public void testChunkedInfer_BatchesInputsIntoASingleRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OciGenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
                { "embeddings": [ [ 0.0123, -0.0123 ], [ 0.0456, -0.0456 ] ], "modelId": "cohere.embed-v4.0" }
                """));

            var model = OciGenAiEmbeddingsModelTests.createModel(getUrl(webServer), EMBEDDINGS_MODEL, null, null, null, null);
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                null,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));
            for (int i = 0; i < 2; i++) {
                assertThat(results.get(i), instanceOf(ChunkedInferenceEmbedding.class));
                var embedding = (ChunkedInferenceEmbedding) results.get(i);
                assertThat(embedding.chunks(), hasSize(1));
                assertThat(embedding.chunks().get(0).embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
            }
            assertTrue(
                Arrays.equals(
                    new float[] { 0.0123f, -0.0123f },
                    ((DenseEmbeddingFloatResults.Embedding) ((ChunkedInferenceEmbedding) results.get(0)).chunks().get(0).embedding())
                        .values()
                )
            );
            assertTrue(
                Arrays.equals(
                    new float[] { 0.0456f, -0.0456f },
                    ((DenseEmbeddingFloatResults.Embedding) ((ChunkedInferenceEmbedding) results.get(1)).chunks().get(0).embedding())
                        .values()
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.get("inputs"), is(List.of("a", "bb")));
            assertThat(requestMap.get("inputType"), is("SEARCH_DOCUMENT"));
        }
    }

    public void testChunkedInfer_NoInputs() throws IOException {
        try (var service = createInferenceService()) {
            var model = OciGenAiEmbeddingsModelTests.createModel(getUrl(webServer), EMBEDDINGS_MODEL, null, null, null, null);
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(model, List.of(), new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            assertThat(listener.actionGet(TIMEOUT), empty());
            assertThat(webServer.requests(), empty());
        }
    }

    public void testRerankInfer_SendsARerankRequest() throws IOException {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
                {
                    "documentRanks": [
                        { "document": "Paris is the capital of France.", "index": 0, "relevanceScore": 0.98 },
                        { "document": "Berlin is in Germany.", "index": 2, "relevanceScore": 0.12 }
                    ],
                    "id": "abc",
                    "modelId": "cohere.rerank-v3.5"
                }
                """));

            var model = OciGenAiRerankModelTests.createModel(getUrl(webServer), RERANK_MODEL, null, null);
            var request = new RerankRequest(
                List.of(
                    InferenceString.ofText("Paris is the capital of France."),
                    InferenceString.ofText("Bananas are yellow."),
                    InferenceString.ofText("Berlin is in Germany.")
                ),
                InferenceString.ofText("capital of france"),
                2,
                true,
                new HashMap<>()
            );

            var listener = new TestPlainActionFuture<InferenceServiceResults>();
            service.rerankInfer(model, request, null, listener);
            var result = listener.actionGet(TIMEOUT);

            var expectedResults = List.of(
                new RankedDocsResultsTests.RerankExpectation(
                    Map.of(INDEX, 0, RELEVANCE_SCORE, 0.98f, TEXT, "Paris is the capital of France.")
                ),
                new RankedDocsResultsTests.RerankExpectation(Map.of(INDEX, 2, RELEVANCE_SCORE, 0.12f, TEXT, "Berlin is in Germany."))
            );
            assertThat(result.asMap(), is(buildExpectationRerank(expectedResults)));

            assertThat(webServer.requests(), hasSize(1));
            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.get("input"), is("capital of france"));
            assertThat(
                requestMap.get("documents"),
                is(List.of("Paris is the capital of France.", "Bananas are yellow.", "Berlin is in Germany."))
            );
            assertThat(requestMap.get("servingMode"), is(Map.of("servingType", "ON_DEMAND", "modelId", RERANK_MODEL)));
            assertThat(requestMap.get("topN"), is(2));
            assertThat(requestMap.get("isEcho"), is(true));
        }
    }

    public void testInfer_Completion_NonStreaming() throws IOException {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(CHAT_RESPONSE));

            var model = OciGenAiChatCompletionModelTests.createCompletionModel(getUrl(webServer), CHAT_MODEL);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, List.of("Say hello."), false, new HashMap<>(), InputType.UNSPECIFIED, null, listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletions(List.of("Hello, how are you today?"))));

            assertThat(webServer.requests(), hasSize(1));
            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.get("servingMode"), is(Map.of("servingType", "ON_DEMAND", "modelId", CHAT_MODEL)));
            assertThat(
                requestMap.get("chatRequest"),
                is(
                    Map.of(
                        "apiFormat",
                        "GENERIC",
                        "isStream",
                        false,
                        "messages",
                        List.of(Map.of("role", "USER", "content", List.of(Map.of("type", "TEXT", "text", "Say hello."))))
                    )
                )
            );
        }
    }

    public void testInfer_Completion_Streaming() throws Exception {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(CHAT_STREAM_RESPONSE));

            var model = OciGenAiChatCompletionModelTests.createCompletionModel(getUrl(webServer), CHAT_MODEL);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, List.of("Say hello."), true, new HashMap<>(), InputType.UNSPECIFIED, null, listener);

            var events = collectEvents(InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream().hasNoErrors());
            assertThat(String.join("", events), is("""
                {"completion":[{"delta":"Hello"},{"delta":", world"}]}"""));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            @SuppressWarnings("unchecked")
            var chatRequest = (Map<String, Object>) requestMap.get("chatRequest");
            assertThat(chatRequest.get("isStream"), is(true));
        }
    }

    public void testUnifiedCompletionInfer_Streaming() throws Exception {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(CHAT_STREAM_RESPONSE));

            var model = OciGenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), CHAT_MODEL);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(model, UnifiedCompletionRequest.streaming(requestBody(null)), null, listener);

            var events = collectEvents(InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream().hasNoErrors());
            var joined = String.join("", events);
            assertThat(joined, containsString("\"delta\":{\"content\":\"Hello\",\"role\":\"assistant\"}"));
            assertThat(joined, containsString("\"delta\":{\"content\":\", world\",\"role\":\"assistant\"}"));
            assertThat(joined, containsString("\"finish_reason\":\"stop\""));
            assertThat(joined, containsString("\"model\":\"" + CHAT_MODEL + "\""));
            assertThat(joined, containsString("\"object\":\"chat.completion.chunk\""));
        }
    }

    public void testUnifiedCompletionInfer_NonStreaming_OverridesTheModelFromTheRequest() throws Exception {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(CHAT_RESPONSE));

            var model = OciGenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), CHAT_MODEL);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                new UnifiedCompletionRequest(requestBody("meta.llama-4-scout-17b-16e-instruct"), false),
                null,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(result, instanceOf(ChatCompletionChunkResponse.class));
            var chunk = (ChatCompletionChunkResponse) result;
            assertThat(chunk.choices().getFirst().message().content(), is("Hello, how are you today?"));
            assertThat(chunk.choices().getFirst().finishReason(), is("stop"));
            assertThat(chunk.object(), is("chat.completion"));
            assertThat(chunk.usage().totalTokens(), is(53));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(
                requestMap.get("servingMode"),
                is(Map.of("servingType", "ON_DEMAND", "modelId", "meta.llama-4-scout-17b-16e-instruct"))
            );
        }
    }

    public void testInfer_ResourceNotFound() throws IOException {
        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(404).setBody("""
                { "code": "404", "message": "Entity with key cohere.does-not-exist not found" }
                """));

            var model = OciGenAiEmbeddingsModelTests.createModel(getUrl(webServer), "cohere.does-not-exist", null, null, null, null);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Resource not found at "));
            assertThat(error.getMessage(), containsString("Error message: [Entity with key cohere.does-not-exist not found]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // configuration
    // ---------------------------------------------------------------------------------------------------------------------------

    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {
            String content = XContentHelper.stripWhitespace(
                Strings.format(
                    """
                        {
                            "service": "ocigenai",
                            "name": "OCI Generative AI",
                            "task_types": ["text_embedding", "rerank", "completion", "chat_completion"],
                            "features": { "non_streaming_chat": { "supported": true } },
                            "configurations": {
                                "region": {
                                    "description": "%s",
                                    "label": "Region",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "compartment_id": {
                                    "description": "The OCID of the compartment the inference requests are authorized against.",
                                    "label": "Compartment OCID",
                                    "required": true,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "model_id": {
                                    "description": "%s",
                                    "label": "Model ID",
                                    "required": true,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "endpoint_id": {
                                    "description": "%s",
                                    "label": "Endpoint OCID",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "url": {
                                    "description": "%s",
                                    "label": "URL",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "dimensions": {
                                    "description": "%s",
                                    "label": "Dimensions",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding"]
                                },
                                "max_input_tokens": {
                                    "description": "Allows you to specify the maximum number of tokens per input.",
                                    "label": "Maximum Input Tokens",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": true,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding"]
                                },
                                "tenancy_id": {
                                    "description": "The OCID of the OCI tenancy that owns the API signing key.",
                                    "label": "Tenancy OCID",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "user_id": {
                                    "description": "The OCID of the OCI user the API signing key belongs to.",
                                    "label": "User OCID",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "fingerprint": {
                                    "description": "The fingerprint of the API signing key's public key, as shown in the OCI Console.",
                                    "label": "Fingerprint",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "private_key": {
                                    "description": "%s",
                                    "label": "Private Key",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                },
                                "rate_limit.requests_per_minute": {
                                    "description": "Minimize the number of rate limit errors.",
                                    "label": "Rate Limit",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                                }
                            }
                        }
                        """,
                    REGION_DESCRIPTION,
                    MODEL_ID_DESCRIPTION,
                    ENDPOINT_ID_DESCRIPTION,
                    URL_DESCRIPTION,
                    DIMENSIONS_DESCRIPTION,
                    PRIVATE_KEY_DESCRIPTION
                )
            );
            InferenceServiceConfiguration configuration = InferenceServiceConfigurationTests.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    public void testBuildModelFromConfigAndSecrets() throws IOException {
        for (var model : List.<Model>of(
            OciGenAiEmbeddingsModelTests.createModel(null, EMBEDDINGS_MODEL, 512, SimilarityMeasure.COSINE, InputType.INGEST, null),
            OciGenAiChatCompletionModelTests.createCompletionModel(null, CHAT_MODEL),
            OciGenAiChatCompletionModelTests.createChatCompletionModel(null, CHAT_MODEL),
            OciGenAiRerankModelTests.createModel(null, RERANK_MODEL, 2, true)
        )) {
            try (var service = createInferenceService()) {
                var resultModel = service.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
                assertThat(resultModel, is(model));
            }
        }
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            "id",
            TaskType.SPARSE_EMBEDDING,
            OciGenAiService.NAME,
            mock(ServiceSettings.class)
        );
        try (var service = createInferenceService()) {
            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                exception.getMessage(),
                is(Strings.format("The [%s] service does not support task type [%s]", OciGenAiService.NAME, TaskType.SPARSE_EMBEDDING))
            );
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // InferenceServiceTestCase hooks
    // ---------------------------------------------------------------------------------------------------------------------------

    @Override
    public InferenceService createInferenceService() {
        return new OciGenAiService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public Model createEmbeddingModel(@Nullable SimilarityMeasure similarity) {
        return OciGenAiEmbeddingsModelTests.createModel(null, EMBEDDINGS_MODEL, null, similarity, null, null);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(OciGenAiService.RERANK_WINDOW_SIZE));
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------------------------

    /**
     * Creates a model that signs its requests with the test key pair and derives the action URL from the given base URL, like a model
     * configured with the {@code url} service setting does.
     */
    private static OciGenAiEmbeddingsModel createSignedEmbeddingsModel(String baseUrl) {
        var serviceSettings = new OciGenAiEmbeddingsServiceSettings(
            OciGenAiTestUtils.commonSettings(
                null,
                COMPARTMENT_ID,
                EMBEDDINGS_MODEL,
                null,
                URI.create(baseUrl),
                new RateLimitSettings(1000)
            ),
            false,
            null,
            null,
            null
        );
        return new OciGenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            OciGenAiService.NAME,
            null,
            serviceSettings,
            OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS,
            OciGenAiTestUtils.createSecretSettings(),
            OciGenAiRequestUtils::signRequest
        );
    }

    private static UnifiedCompletionRequestBody requestBody(@Nullable String modelId) {
        return new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("Say hello."), "user", null, null, null, null)),
            modelId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static List<String> collectEvents(InferenceEventsAssertion assertion) {
        var events = new ArrayList<String>();
        assertion.events().forEachRemaining(events::add);
        return events;
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }
}
