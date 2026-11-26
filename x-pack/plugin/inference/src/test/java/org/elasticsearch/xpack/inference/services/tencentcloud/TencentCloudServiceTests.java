/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfigurationTests;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudMultimodalEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudTextEmbeddingServiceSettings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TencentCloudServiceTests extends InferenceServiceTestCase {

    private static final String MODEL_ID = "bge-m3";
    private static final String API_KEY = "sk-12345";
    private static final String INFERENCE_ID = "inference-id";
    private static final String DEFAULT_REGION = "bj";
    private static final String IMAGE_URL = "https://example.com/a.png";
    private static final String VIDEO_DATA = "data:video/mp4;base64,abcd";

    public void testParseRequestConfig_MissingApiKey_Fails() throws IOException {
        parseRequestConfig(TaskType.TEXT_EMBEDDING, """
            {
              "service_settings": {
                "model_id": "bge-m3"
              }
            }
            """, assertNoSuccessListener(e -> {
            assertThat(e, instanceOf(ValidationException.class));
            assertThat(e.getMessage(), containsString("api_key"));
        }));
    }

    public void testParseRequestConfig_MissingModelId_Fails() throws IOException {
        parseRequestConfig(TaskType.TEXT_EMBEDDING, """
            {
              "service_settings": {
                "api_key": "sk-12345"
              }
            }
            """, assertNoSuccessListener(e -> {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("model_id"));
        }));
    }

    public void testParseRequestConfig_EmbeddingTask_CreatesMultimodalModel() throws IOException {
        parseRequestConfig(TaskType.EMBEDDING, """
            {
              "service_settings": {
                "model_id": "EmbeddingGemma-2",
                "api_key": "sk-12345"
              },
              "task_settings": {
                "input_type": "ingest",
                "prompt_name": "Document"
              }
            }
            """, ActionListener.wrap(model -> {
            assertThat(model, instanceOf(TencentCloudEmbeddingsModel.class));
            var embeddingsModel = (TencentCloudEmbeddingsModel) model;
            assertThat(embeddingsModel.getTaskType(), is(TaskType.EMBEDDING));
            assertThat(embeddingsModel.getServiceSettings(), instanceOf(TencentCloudEmbeddingServiceSettings.class));
            assertThat(
                embeddingsModel.getMultimodalTaskSettings(),
                is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, "Document"))
            );
            assertThat(embeddingsModel.uri().toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/multimodal-embeddings"));
        }, e -> fail("Model parsing failed: " + e)));
    }

    public void testParseRequestConfig_TextEmbeddingTask_RejectsTaskSettings() throws IOException {
        parseRequestConfig(TaskType.TEXT_EMBEDDING, """
            {
              "service_settings": {
                "model_id": "bge-m3",
                "api_key": "sk-12345"
              },
              "task_settings": {
                "prompt_name": "Document"
              }
            }
            """, assertNoSuccessListener(e -> assertThat(e.getMessage(), containsString("Configuration contains settings"))));
    }

    public void testEmbeddingInfer_SendsMultimodalRequestAndRestoresInputOrder() throws IOException {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {
              "object": "multimodal_embedding",
              "data": {
                "text_embeddings": [
                  { "object": "embedding", "embedding": [0.1, 0.2], "index": 0 },
                  { "object": "embedding", "embedding": [0.3, 0.4], "index": 1 }
                ],
                "image_embeddings": [ { "object": "embedding", "embedding": [0.5, 0.6], "index": 0 } ],
                "video_embeddings": [ { "object": "embedding", "embedding": [0.7, 0.8], "index": 0 } ]
              },
              "model": "EmbeddingGemma-2",
              "usage": { "total_tokens": 562 }
            }
            """));

        var inputs = List.of(
            new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.URL, IMAGE_URL)),
            new InferenceStringGroup("first"),
            new InferenceStringGroup(new InferenceString(DataType.VIDEO, DataFormat.BASE64, VIDEO_DATA)),
            new InferenceStringGroup("second")
        );
        var result = embeddingInfer("EmbeddingGemma-2", inputs, InputType.SEARCH);

        assertThat(
            result.embeddings(),
            is(
                List.of(
                    EmbeddingFloatResults.Embedding.of(List.of(0.5F, 0.6F)),
                    EmbeddingFloatResults.Embedding.of(List.of(0.1F, 0.2F)),
                    EmbeddingFloatResults.Embedding.of(List.of(0.7F, 0.8F)),
                    EmbeddingFloatResults.Embedding.of(List.of(0.3F, 0.4F))
                )
            )
        );
        assertThat(webServer.requests(), hasSize(1));
        var request = webServer.requests().getFirst();
        assertThat(
            request.getHeader(HttpHeaders.AUTHORIZATION),
            is("Bearer " + TencentCloudServiceParameterizedTestConfiguration.API_KEY_VALUE)
        );
        assertThat(
            entityAsMap(request.getBody()),
            is(
                Map.of(
                    "model",
                    "EmbeddingGemma-2",
                    "texts",
                    List.of("first", "second"),
                    "image_url",
                    List.of(IMAGE_URL),
                    "video_data",
                    List.of(VIDEO_DATA),
                    "prompt_name",
                    "SearchQuery"
                )
            )
        );
    }

    public void testEmbeddingInfer_DoesNotSendPromptNameForWeMM() throws IOException {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {
              "data": { "text_embeddings": [ { "embedding": [0.1, 0.2], "index": 0 } ] }
            }
            """));

        embeddingInfer("WeMM-Embedding-2B", List.of(new InferenceStringGroup("text")), InputType.INGEST);

        assertThat(
            entityAsMap(webServer.requests().getFirst().getBody()),
            is(Map.of("model", "WeMM-Embedding-2B", "texts", List.of("text")))
        );
    }

    public void testEmbeddingInfer_SplitsInputsIntoBatches() throws IOException {
        var batchResponse = new StringBuilder("{\"data\": {\"image_embeddings\": [");
        for (int i = 0; i < 8; i++) {
            batchResponse.append(i == 0 ? "" : ",").append("{\"embedding\": [").append(i).append(".0], \"index\": ").append(i).append("}");
        }
        batchResponse.append("]}}");
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(batchResponse.toString()));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(batchResponse.toString()));

        var inputs = IntStream.range(0, 16)
            .mapToObj(i -> new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.URL, IMAGE_URL + "?i=" + i)))
            .toList();
        var result = embeddingInfer("WeMM-Embedding-2B", inputs, InputType.UNSPECIFIED);

        assertThat(result.embeddings(), hasSize(16));
        assertThat(webServer.requests(), hasSize(2));
        for (var request : webServer.requests()) {
            assertThat((List<?>) entityAsMap(request.getBody()).get("image_url"), hasSize(8));
        }
    }

    public void testMergeEmbeddingResults_ConcatenatesBatchesInOrder() {
        var batchResults = new AtomicReferenceArray<InferenceServiceResults>(2);
        batchResults.set(0, new GenericDenseEmbeddingFloatResults(List.of(EmbeddingFloatResults.Embedding.of(List.of(1F)))));
        batchResults.set(
            1,
            new GenericDenseEmbeddingFloatResults(
                List.of(EmbeddingFloatResults.Embedding.of(List.of(2F)), EmbeddingFloatResults.Embedding.of(List.of(3F)))
            )
        );

        var merged = (GenericDenseEmbeddingFloatResults) TencentCloudService.mergeEmbeddingResults(batchResults);

        assertThat(
            merged.embeddings(),
            is(
                List.of(
                    EmbeddingFloatResults.Embedding.of(List.of(1F)),
                    EmbeddingFloatResults.Embedding.of(List.of(2F)),
                    EmbeddingFloatResults.Embedding.of(List.of(3F))
                )
            )
        );
    }

    public void testEmbeddingInfer_RejectsUnsupportedInputs() throws IOException {
        var unsupportedInputs = Map.of(
            new InferenceString(DataType.AUDIO, DataFormat.BASE64, "data:audio/mpeg;base64,abcd"),
            "The tencentcloud service does not support [audio] inputs, found at $.input[1]",
            new InferenceString(DataType.PDF, DataFormat.URL, "https://example.com/a.pdf"),
            "The tencentcloud service does not support [pdf] inputs, found at $.input[1]",
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/bmp;base64,abcd"),
            "The tencentcloud service only supports base64 images of type [png, jpeg, gif, webp], found an unsupported image at $.input[1]",
            new InferenceString(DataType.VIDEO, DataFormat.BASE64, "data:video/webm;base64,abcd"),
            "The tencentcloud service only supports base64 videos of type [mp4], found an unsupported video at $.input[1]"
        );
        try (var service = createService()) {
            for (var entry : unsupportedInputs.entrySet()) {
                var listener = new PlainActionFuture<InferenceServiceResults>();
                service.embeddingInfer(
                    createMultimodalModel("WeMM-Embedding-2B"),
                    new EmbeddingRequest(
                        List.of(new InferenceStringGroup("text"), new InferenceStringGroup(entry.getKey())),
                        null,
                        Map.of()
                    ),
                    TEST_REQUEST_TIMEOUT,
                    listener
                );
                var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
                assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
                assertThat(exception.getMessage(), is(entry.getValue()));
            }
        }
        assertThat(webServer.requests(), empty());
    }

    public void testEmbeddingInfer_DoesNotRetryInvalidParameterValue() throws IOException {
        webServer.enqueue(new MockResponse().setResponseCode(500).setBody("""
            {"error":{"message":"ImageType not supported.","type":"api_error","code":"InvalidParameterValue"}}"""));

        try (var service = createService()) {
            var listener = new PlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                createMultimodalModel("WeMM-Embedding-2B"),
                new EmbeddingRequest(List.of(new InferenceStringGroup("text")), null, Map.of()),
                TEST_REQUEST_TIMEOUT,
                listener
            );
            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.getMessage(), containsString("ImageType not supported."));
        }
        assertThat(webServer.requests(), hasSize(1));
    }

    public void testEmbeddingInferRetriesServerErrors() throws IOException {
        webServer.enqueue(new MockResponse().setResponseCode(randomFrom(500, 502)).setBody("""
            {"error":{"message":"Temporary upstream failure","code":"InternalError"}}"""));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {"data":{"text_embeddings":[{"embedding":[0.1,0.2],"index":0}]}}"""));

        assertThat(
            embeddingInfer("WeMM-Embedding-2B", List.of(new InferenceStringGroup("text")), InputType.SEARCH).embeddings(),
            hasSize(1)
        );
        assertThat(webServer.requests(), hasSize(2));
    }

    public void testEmbeddingInferRejectsGroupedContent() throws IOException {
        try (var service = createService()) {
            var listener = new PlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                createMultimodalModel("WeMM-Embedding-2B"),
                new EmbeddingRequest(
                    List.of(
                        new InferenceStringGroup(
                            List.of(new InferenceString(DataType.TEXT, "first"), new InferenceString(DataType.TEXT, "second"))
                        )
                    ),
                    null,
                    Map.of()
                ),
                TEST_REQUEST_TIMEOUT,
                listener
            );
            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
            assertThat(exception.getMessage(), containsString("must contain a single item"));
        }
        assertThat(webServer.requests(), empty());
    }

    public void testParseRequestConfig_EmbeddingTask_RejectsClusterWithoutEmbeddingTask() throws IOException {
        try (var service = createService(TencentCloudService.TENCENT_CLOUD_INFERENCE_SERVICE_ADDED)) {
            service.parseRequestConfig(INFERENCE_ID, TaskType.EMBEDDING, embeddingConfig(), assertNoSuccessListener(e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), containsString("All nodes in the cluster are not aware of the [embedding] task type"));
            }));
        }
    }

    public void testParseRequestConfig_TextEmbeddingTask_AllowedOnClusterWithoutEmbeddingTask() throws IOException {
        try (var service = createService(TencentCloudService.TENCENT_CLOUD_INFERENCE_SERVICE_ADDED)) {
            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig(INFERENCE_ID, TaskType.TEXT_EMBEDDING, map("""
                {
                  "service_settings": { "model_id": "bge-m3", "api_key": "sk-12345" }
                }
                """), listener);
            assertThat(listener.actionGet(TEST_REQUEST_TIMEOUT).getTaskType(), is(TaskType.TEXT_EMBEDDING));
        }
    }

    public void testChunkedInfer_EmbeddingTask_SplitsBatchesByModalityLimits() throws IOException {
        // Each response returns one embedding per image URL of the request that produced it, keyed by the index in the URL
        Function<MockRequest, String> bodyGenerator = request -> {
            var urls = imageUrls(request);
            var embeddings = new StringBuilder();
            for (int i = 0; i < urls.size(); i++) {
                embeddings.append(i == 0 ? "" : ",")
                    .append("{\"embedding\": [")
                    .append(imageIndex(urls.get(i)))
                    .append(".0, ")
                    .append(imageIndex(urls.get(i)))
                    .append(".5], \"index\": ")
                    .append(i)
                    .append("}");
            }
            return "{\"data\": {\"image_embeddings\": [" + embeddings + "]}}";
        };
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(bodyGenerator));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(bodyGenerator));

        var inputs = IntStream.range(0, 10)
            .mapToObj(i -> IMAGE_URL + "?i=" + i)
            .map(url -> new ChunkInferenceInput(new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.URL, url)), null))
            .toList();

        List<ChunkedInference> results;
        try (var service = createService()) {
            var listener = new PlainActionFuture<List<ChunkedInference>>();
            service.chunkedInfer(
                createMultimodalModel("WeMM-Embedding-2B"),
                inputs,
                Map.of(),
                InputType.UNSPECIFIED,
                TEST_REQUEST_TIMEOUT,
                listener
            );
            results = listener.actionGet(TEST_REQUEST_TIMEOUT);
        }

        assertThat(webServer.requests(), hasSize(2));
        var batchSizes = webServer.requests().stream().map(request -> imageUrls(request).size()).sorted().toList();
        assertThat(batchSizes, is(List.of(2, 8)));

        // The batches are sent concurrently, so the embeddings are checked per input against the vector derived from its own URL
        assertThat(results, hasSize(inputs.size()));
        for (int i = 0; i < inputs.size(); i++) {
            var url = inputs.get(i).input().value().value();
            assertThat(results.get(i), instanceOf(ChunkedInferenceEmbedding.class));
            var chunk = ((ChunkedInferenceEmbedding) results.get(i)).chunks().getFirst();
            assertThat(chunk.offset(), is(new ChunkedInference.TextOffset(0, url.length())));
            assertThat(chunk.embedding(), is(EmbeddingFloatResults.Embedding.of(List.of((float) imageIndex(url), imageIndex(url) + 0.5F))));
        }
    }

    private static List<String> imageUrls(MockRequest request) {
        try {
            var body = entityAsMap(request.getBody());
            return body.containsKey("image_url") ? ((List<?>) body.get("image_url")).stream().map(Object::toString).toList() : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int imageIndex(String url) {
        return Integer.parseInt(url.substring(url.indexOf("?i=") + 3));
    }

    public void testChunkedInfer_EmbeddingTask_RejectsUnsupportedInputs() throws IOException {
        var inputs = List.of(
            new ChunkInferenceInput(new InferenceStringGroup("text"), null),
            new ChunkInferenceInput(
                new InferenceStringGroup(new InferenceString(DataType.AUDIO, DataFormat.BASE64, "data:audio/mpeg;base64,abcd")),
                null
            )
        );

        try (var service = createService()) {
            var listener = new PlainActionFuture<List<ChunkedInference>>();
            service.chunkedInfer(
                createMultimodalModel("WeMM-Embedding-2B"),
                inputs,
                Map.of(),
                InputType.UNSPECIFIED,
                TEST_REQUEST_TIMEOUT,
                listener
            );
            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
            assertThat(exception.getMessage(), containsString("does not support [audio] inputs"));
        }
        assertThat(webServer.requests(), empty());
    }

    public void testEmbeddingInfer_TruncatesTextsToMaxInputTokens() throws IOException {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {"data":{"image_embeddings":[{"embedding":[0.1,0.2],"index":0}],"text_embeddings":[{"embedding":[0.3,0.4],"index":0}]}}"""));

        var inputs = List.of(
            new InferenceStringGroup("abcdef"),
            new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;base64,abcdef"))
        );
        try (var service = createService()) {
            var listener = new PlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                createMultimodalModel("WeMM-Embedding-2B", 1),
                new EmbeddingRequest(inputs, InputType.UNSPECIFIED, Map.of()),
                TEST_REQUEST_TIMEOUT,
                listener
            );
            assertThat(listener.actionGet(TEST_REQUEST_TIMEOUT), instanceOf(GenericDenseEmbeddingFloatResults.class));
        }

        var body = entityAsMap(webServer.requests().getFirst().getBody());
        // One token is three characters
        assertThat(body.get("texts"), is(List.of("abc")));
        assertThat(body.get("image_data"), is(List.of("data:image/png;base64,abcdef")));
    }

    private GenericDenseEmbeddingFloatResults embeddingInfer(String modelId, List<InferenceStringGroup> inputs, InputType inputType)
        throws IOException {
        try (var service = createService()) {
            var listener = new PlainActionFuture<InferenceServiceResults>();
            service.embeddingInfer(
                createMultimodalModel(modelId),
                new EmbeddingRequest(inputs, inputType, Map.of()),
                TEST_REQUEST_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(result, instanceOf(GenericDenseEmbeddingFloatResults.class));
            return (GenericDenseEmbeddingFloatResults) result;
        }
    }

    private Map<String, Object> embeddingConfig() throws IOException {
        return map("""
            {
              "service_settings": { "model_id": "EmbeddingGemma-2", "api_key": "sk-12345" }
            }
            """);
    }

    private TencentCloudEmbeddingsModel createMultimodalModel(String modelId) {
        return createMultimodalModel(modelId, null);
    }

    private TencentCloudEmbeddingsModel createMultimodalModel(String modelId, @Nullable Integer maxInputTokens) {
        return new TencentCloudEmbeddingsModel(
            INFERENCE_ID,
            TaskType.EMBEDDING,
            new TencentCloudEmbeddingServiceSettings(modelId, null, null, null, null, maxInputTokens),
            TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString(TencentCloudServiceParameterizedTestConfiguration.API_KEY_VALUE.toCharArray())),
            URI.create(getUrl(webServer))
        );
    }

    public void testRerankerWindowSize_ReturnsConservativeValue() throws IOException {
        try (var service = createService()) {
            assertThat(service.rerankerWindowSize("bge-reranker-v2-m3"), is(350));
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                        "service": "tencentcloud",
                        "name": "TencentCloud AI Gateway",
                        "task_types": ["text_embedding", "rerank", "completion", "chat_completion", "embedding"],
                        "configurations": {
                            "model_id": {
                                "description": "The name of the model to use for the inference task, e.g. bge-m3 (text embeddings), WeMM-Embedding-2B (multimodal embeddings), deepseek-v3 (chat/completions), bge-reranker-v2-m3 (rerank). The gateway supports additional models; check the TencentCloud AI Gateway documentation for the full list.",
                                "label": "Model ID",
                                "required": true,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion", "embedding"]
                            },
                            "region": {
                                "default_value": "bj",
                                "description": "The TencentCloud AI Gateway region, e.g. bj, sh, gz. The endpoint URL is constructed as https://{region}.aisearch.tencentelasticsearch.com/v1/<task-path>.",
                                "label": "Region",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion", "embedding"]
                            },
                            "api_key": {
                                "description": "The TencentCloud AI Gateway API key. Contact the administrator to obtain a token in the format sk-<your-api-key>.",
                                "label": "API Key",
                                "required": true,
                                "sensitive": true,
                                "updatable": true,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion", "embedding"]
                            },
                            "rate_limit.requests_per_minute": {
                                "description": "Minimize the number of rate limit errors.",
                                "label": "Rate Limit",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "int",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion", "embedding"]
                            }
                        }
                    }
                    """
            );
            var configuration = InferenceServiceConfigurationTests.fromXContentBytes(new BytesArray(content), XContentType.JSON);
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            var serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("bge-reranker-v2-m3"), is(350));
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        var serviceSettings = new TencentCloudTextEmbeddingServiceSettings(
            MODEL_ID,
            DEFAULT_REGION,
            new RateLimitSettings(20),
            similarity,
            null,
            null
        );
        return new TencentCloudEmbeddingsModel(
            INFERENCE_ID,
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString(API_KEY))
        );
    }

    private TencentCloudService createService() {
        return createService(TransportVersion.current());
    }

    private TencentCloudService createService(TransportVersion minTransportVersion) {
        return new TencentCloudService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            TencentCloudServiceParameterizedTestConfiguration.mockClusterServiceWithMinTransportVersion(minTransportVersion)
        );
    }

    @Override
    public InferenceService createInferenceService() {
        return createService();
    }

    private void parseRequestConfig(TaskType taskType, String json, ActionListener<Model> listener) throws IOException {
        try (var service = createService()) {
            service.parseRequestConfig(INFERENCE_ID, taskType, map(json), listener);
        }
    }

    private Map<String, Object> map(String json) throws IOException {
        try (
            var parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.getBytes(StandardCharsets.UTF_8))
        ) {
            return parser.map();
        }
    }

}
