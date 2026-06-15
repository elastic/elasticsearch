/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponse;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModelBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerServiceSettings;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStreamSchema;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.action.ActionListener.assertOnce;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.core.TimeValue.THIRTY_SECONDS;
import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.xpack.core.inference.action.UnifiedCompletionRequestTests.randomTextInputOnlyUnifiedCompletionRequest;
import static org.elasticsearch.xpack.core.inference.action.UnifiedCompletionRequestTests.randomUnifiedCompletionRequest;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.ACCESS_KEY_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.SECRET_KEY_FIELD;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SageMakerServiceTests extends InferenceServiceTestCase {

    private static final String QUERY = "query";
    private static final List<String> INPUT = List.of("input");
    private static final InputType INPUT_TYPE = InputType.UNSPECIFIED;

    private SageMakerModelBuilder modelBuilder;
    private SageMakerClient client;
    private SageMakerSchemas schemas;
    private SageMakerService sageMakerService;
    private ThreadPool sageMakerThreadPool;

    @Override
    public void doInit() throws IOException {
        super.doInit();
        modelBuilder = mock();
        client = mock();
        schemas = mock();
        sageMakerThreadPool = mock();
        when(sageMakerThreadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(sageMakerThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        sageMakerService = new SageMakerService(modelBuilder, client, schemas, sageMakerThreadPool, Map::of, mockClusterServiceEmpty());
    }

    public void testSupportedTaskTypes() {
        sageMakerService.supportedTaskTypes();
        verify(schemas, only()).supportedTaskTypes();
    }

    public void testSupportedStreamingTasks() {
        sageMakerService.supportedStreamingTasks();
        verify(schemas, only()).supportedStreamingTasks();
    }

    public void testParseRequestConfig() {
        sageMakerService.parseRequestConfig("modelId", TaskType.ANY, Map.of(), assertNoFailureListener(model -> {
            verify(modelBuilder, only()).fromRequest(eq("modelId"), eq(TaskType.ANY), eq(SageMakerService.NAME), eq(Map.of()));
        }));
    }

    public void testParsePersistedConfig_WithSecrets() {
        sageMakerService.parsePersistedConfig(
            new UnparsedModel("modelId", TaskType.ANY, SageMakerService.NAME, Map.of(), Map.of("key", "value"))
        );
        verify(modelBuilder, only()).fromStorage(
            eq("modelId"),
            eq(TaskType.ANY),
            eq(SageMakerService.NAME),
            eq(Map.of()),
            eq(Map.of("key", "value"))
        );
    }

    public void testParseRequestConfig_TextEmbedding_DimensionsSetByUser_ReturnsError() {
        // dimensions_set_by_user is internal, so supplying it in a request must be rejected as an unknown setting.
        var realModelBuilder = new SageMakerModelBuilder(new SageMakerSchemas());
        var service = new SageMakerService(realModelBuilder, client, schemas, sageMakerThreadPool, Map::of, mockClusterServiceEmpty());
        TestPlainActionFuture<Model> listener = new TestPlainActionFuture<>();
        var serviceSettings = new HashMap<String, Object>(
            Map.of(
                ACCESS_KEY_FIELD,
                "api_key",
                SECRET_KEY_FIELD,
                "secret_key",
                "endpoint_name",
                "endpoint",
                "api",
                "openai",
                "region",
                "region",
                DIMENSIONS_SET_BY_USER,
                true
            )
        );
        service.parseRequestConfig(
            "modelId",
            TaskType.TEXT_EMBEDDING,
            new HashMap<String, Object>(Map.of("service_settings", serviceSettings)),
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is("Configuration contains settings [{dimensions_set_by_user=true}] unknown to the [" + SageMakerService.NAME + "] service")
        );
    }

    public void testParsePersistedConfig_TextEmbedding_DimensionsSetByUser_DoesNotError() {
        // A persisted config legitimately stores dimensions_set_by_user, so parsing it back must not error.
        var realModelBuilder = new SageMakerModelBuilder(new SageMakerSchemas());
        var service = new SageMakerService(realModelBuilder, client, schemas, sageMakerThreadPool, Map::of, mockClusterServiceEmpty());
        var serviceSettings = new HashMap<String, Object>(
            Map.of("endpoint_name", "endpoint", "api", "openai", "region", "region", "dimensions", 123, DIMENSIONS_SET_BY_USER, false)
        );
        var config = new HashMap<String, Object>(
            Map.of("service_settings", serviceSettings, "task_settings", new HashMap<String, Object>())
        );

        var model = service.parsePersistedConfig(
            new UnparsedModel("modelId", TaskType.TEXT_EMBEDDING, SageMakerService.NAME, config, null)
        );

        assertThat(((SageMakerServiceSettings) model.getServiceSettings()).dimensionsSetByUser(), is(false));
    }

    private static Model mockUnsupportedModel() {
        Model model = mock();
        ModelConfigurations modelConfigurations = mock();
        when(modelConfigurations.getService()).thenReturn("mockService");
        when(modelConfigurations.getInferenceEntityId()).thenReturn("mockInferenceId");
        when(model.getConfigurations()).thenReturn(modelConfigurations);
        return model;
    }

    private static <T> ActionListener<T> assertUnsupportedModel() {
        return assertNoSuccessListener(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(
                e.getMessage(),
                equalTo(
                    "The internal model was invalid, please delete the service [mockService] "
                        + "with id [mockInferenceId] and add it again."
                )
            );
            assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        });
    }

    public void testInfer() {
        var model = mockModel();

        SageMakerSchema schema = mock();
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvoke();

        sageMakerService.infer(model, INPUT, false, null, INPUT_TYPE, THIRTY_SECONDS, assertNoFailureListener(ignored -> {
            verify(schemas, only()).schemaFor(eq(model));
            verify(schema, times(1)).request(eq(model), assertRequest());
            verify(schema, times(1)).response(eq(model), any(), any());
        }));
        verify(client, only()).invoke(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testRerankInfer() {
        var model = mockModel();

        SageMakerSchema schema = mock();
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvoke();

        var returnDocuments = randomOptionalBoolean();
        var topN = randomNonNegativeIntOrNull();
        sageMakerService.rerankInfer(
            model,
            new RerankRequest(InferenceString.fromStringList(INPUT), InferenceString.ofText(QUERY), topN, returnDocuments, null),
            THIRTY_SECONDS,
            assertNoFailureListener(ignored -> {
                verify(schemas, only()).schemaFor(eq(model));
                verify(schema, times(1)).request(eq(model), assertRerankRequest(returnDocuments, topN));
                verify(schema, times(1)).response(eq(model), any(), any());
            })
        );
        verify(client, only()).invoke(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testRerankInfer_ThrowsError_WithNonTextQuery() throws IOException {
        var textInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(textInputs, nonTextQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputs() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var textQuery = createRandomUsingDataTypes(EnumSet.of(DataType.TEXT));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, textQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputsAndQuery() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, nonTextQuery);
    }

    private void testRerankInfer_ThrowsError_WithNonTextInputOrQuery(List<InferenceString> inputs, InferenceString query)
        throws IOException {
        var model = mock(SageMakerModel.class);

        try (var service = createInferenceService()) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.rerankInfer(model, new RerankRequest(inputs, query, null, null, new HashMap<>()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
            assertThat(
                thrownException.getMessage(),
                is("The amazon_sagemaker service does not support rerank with non-text inputs or queries")
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void test_nullTimeoutUsesClusterSetting() throws InterruptedException {
        var model = mockModel();
        when(schemas.schemaFor(model)).thenReturn(mock());

        var configuredTimeout = TimeValue.timeValueSeconds(15);
        var clusterService = mockClusterService(
            Settings.builder().put(InferencePlugin.INFERENCE_QUERY_TIMEOUT.getKey(), configuredTimeout).build()
        );

        var service = new SageMakerService(modelBuilder, client, schemas, sageMakerThreadPool, Map::of, clusterService);

        var capturedTimeout = new AtomicReference<TimeValue>();
        doAnswer(ans -> {
            capturedTimeout.set(ans.getArgument(2));
            ((ActionListener<InvokeEndpointResponse>) ans.getArgument(4)).onResponse(null);
            return null;
        }).when(client).invoke(any(), any(), any(), any(), any());

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(ActionListener.<InferenceServiceResults>noop(), latch);
        service.infer(model, INPUT, false, null, InputType.SEARCH, null, latchedListener);

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(configuredTimeout, capturedTimeout.get());
    }

    @SuppressWarnings("unchecked")
    public void test_providedTimeoutPropagateProperly() throws InterruptedException {
        var model = mockModel();
        when(schemas.schemaFor(model)).thenReturn(mock());

        var providedTimeout = TimeValue.timeValueSeconds(45);
        var clusterService = mockClusterService(
            Settings.builder().put(InferencePlugin.INFERENCE_QUERY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(15)).build()
        );

        var service = new SageMakerService(modelBuilder, client, schemas, sageMakerThreadPool, Map::of, clusterService);

        var capturedTimeout = new AtomicReference<TimeValue>();
        doAnswer(ans -> {
            capturedTimeout.set(ans.getArgument(2));
            ((ActionListener<InvokeEndpointResponse>) ans.getArgument(4)).onResponse(null);
            return null;
        }).when(client).invoke(any(), any(), any(), any(), any());

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(ActionListener.<InferenceServiceResults>noop(), latch);
        service.infer(model, INPUT, false, null, InputType.SEARCH, providedTimeout, latchedListener);

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(providedTimeout, capturedTimeout.get());
    }

    private SageMakerModel mockModel() {
        SageMakerModel model = mock();
        when(model.override(any())).thenReturn(model);
        when(model.awsSecretSettings()).thenReturn(
            Optional.of(new AwsSecretSettings(new SecureString("test-accessKey"), new SecureString("test-secretKey")))
        );
        return model;
    }

    private void mockInvoke() {
        doAnswer(ans -> {
            ActionListener<InvokeEndpointResponse> responseListener = ans.getArgument(4);
            responseListener.onResponse(InvokeEndpointResponse.builder().build());
            return null; // Void
        }).when(client).invoke(any(), any(), any(), any(), any());
    }

    private static SageMakerInferenceRequest assertRequest() {
        return assertArg(request -> {
            assertThat(request.query(), is(nullValue()));
            assertThat(request.input(), containsInAnyOrder(INPUT.toArray()));
            assertThat(request.inputType(), equalTo(InputType.UNSPECIFIED));
            assertNull(request.returnDocuments());
            assertNull(request.topN());
        });
    }

    private static SageMakerInferenceRequest assertRerankRequest(Boolean returnDocuments, Integer topN) {
        return assertArg(request -> {
            assertThat(request.query(), is(QUERY));
            assertThat(request.input(), is(INPUT));
            assertThat(request.inputType(), is(InputType.UNSPECIFIED));
            assertThat(request.returnDocuments(), is(returnDocuments));
            assertThat(request.topN(), is(topN));
        });
    }

    public void testInferStream() {
        SageMakerModel model = mockModel();

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        mockInvokeStream();

        sageMakerService.infer(model, INPUT, true, null, INPUT_TYPE, THIRTY_SECONDS, assertNoFailureListener(ignored -> {
            verify(schemas, only()).streamSchemaFor(eq(model));
            verify(schema, times(1)).streamRequest(eq(model), assertRequest());
            verify(schema, times(1)).streamResponse(eq(model), any());
        }));
        verify(client, only()).invokeStream(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private void mockInvokeStream() {
        doAnswer(ans -> {
            ActionListener<SageMakerClient.SageMakerStream> responseListener = ans.getArgument(4);
            responseListener.onResponse(
                new SageMakerClient.SageMakerStream(InvokeEndpointWithResponseStreamResponse.builder().build(), mock())
            );
            return null; // Void
        }).when(client).invokeStream(any(), any(), any(), any(), any());
    }

    public void testInferError() {
        SageMakerModel model = mockModel();

        var expectedException = new IllegalArgumentException("hola");
        SageMakerSchema schema = mock();
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvokeFailure(expectedException);

        sageMakerService.infer(model, INPUT, false, null, INPUT_TYPE, THIRTY_SECONDS, assertNoSuccessListener(ignored -> {
            verify(schemas, only()).schemaFor(eq(model));
            verify(schema, times(1)).request(eq(model), assertRequest());
            verify(schema, times(1)).error(eq(model), assertArg(e -> assertThat(e, equalTo(expectedException))));
        }));
        verify(client, only()).invoke(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private void mockInvokeFailure(Exception e) {
        doAnswer(ans -> {
            ActionListener<?> responseListener = ans.getArgument(4);
            responseListener.onFailure(e);
            return null; // Void
        }).when(client).invoke(any(), any(), any(), any(), any());
    }

    public void testInferException() {
        SageMakerModel model = mockModel();
        when(model.getInferenceEntityId()).thenReturn("some id");

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        doThrow(new IllegalArgumentException("wow, really?")).when(client).invokeStream(any(), any(), any(), any(), any());

        sageMakerService.infer(model, INPUT, true, null, INPUT_TYPE, THIRTY_SECONDS, assertNoSuccessListener(e -> {
            verify(schemas, only()).streamSchemaFor(eq(model));
            verify(schema, times(1)).streamRequest(eq(model), assertRequest());
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(e.getMessage(), equalTo("Failed to call SageMaker for inference id [some id]."));
        }));
        verify(client, only()).invokeStream(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testUnifiedInferWithWrongModel() {
        sageMakerService.unifiedCompletionInfer(
            mockUnsupportedModel(),
            randomUnifiedCompletionRequest(),
            THIRTY_SECONDS,
            assertUnsupportedModel()
        );
    }

    public void testUnifiedInfer() {
        var model = mockModel();

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        mockInvokeStream();

        sageMakerService.unifiedCompletionInfer(
            model,
            randomTextInputOnlyUnifiedCompletionRequest(),
            THIRTY_SECONDS,
            assertNoFailureListener(ignored -> {
                verify(schemas, only()).streamSchemaFor(eq(model));
                verify(schema, times(1)).chatCompletionStreamRequest(eq(model), any());
                verify(schema, times(1)).chatCompletionStreamResponse(eq(model), any());
            })
        );
        verify(client, only()).invokeStream(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testUnifiedInferError() {
        var model = mockModel();

        var expectedException = new IllegalArgumentException("hola");
        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        mockInvokeStreamFailure(expectedException);

        sageMakerService.unifiedCompletionInfer(
            model,
            randomTextInputOnlyUnifiedCompletionRequest(),
            THIRTY_SECONDS,
            assertNoSuccessListener(ignored -> {
                verify(schemas, only()).streamSchemaFor(eq(model));
                verify(schema, times(1)).chatCompletionStreamRequest(eq(model), any());
                verify(schema, times(1)).chatCompletionError(eq(model), assertArg(e -> assertThat(e, equalTo(expectedException))));
            })
        );
        verify(client, only()).invokeStream(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private void mockInvokeStreamFailure(Exception e) {
        doAnswer(ans -> {
            ActionListener<?> responseListener = ans.getArgument(4);
            responseListener.onFailure(e);
            return null; // Void
        }).when(client).invokeStream(any(), any(), any(), any(), any());
    }

    public void testUnifiedInferException() {
        SageMakerModel model = mockModel();
        when(model.getInferenceEntityId()).thenReturn("some id");

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        doThrow(new IllegalArgumentException("wow, rude")).when(client).invokeStream(any(), any(), any(), any(), any());

        sageMakerService.unifiedCompletionInfer(
            model,
            randomTextInputOnlyUnifiedCompletionRequest(),
            THIRTY_SECONDS,
            assertNoSuccessListener(e -> {
                verify(schemas, only()).streamSchemaFor(eq(model));
                verify(schema, times(1)).chatCompletionStreamRequest(eq(model), any());
                assertThat(e, isA(ElasticsearchStatusException.class));
                assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                assertThat(e.getMessage(), equalTo("Failed to call SageMaker for inference id [some id]."));
            })
        );
        verify(client, only()).invokeStream(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testChunkedInferWithWrongModel() {
        sageMakerService.chunkedInfer(
            mockUnsupportedModel(),
            INPUT.stream().map(ChunkInferenceInput::new).toList(),
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertUnsupportedModel()
        );
    }

    public void testChunkedInfer() throws Exception {
        var model = mockModelForChunking();

        SageMakerSchema schema = mock();
        when(schema.response(any(), any(), any())).thenReturn(DenseEmbeddingFloatResultsTests.createRandomResults());
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvoke();

        var expectedInput = Set.of("first", "second");

        sageMakerService.chunkedInfer(
            model,
            expectedInput.stream().map(ChunkInferenceInput::new).toList(),
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertOnce(assertNoFailureListener(chunkedInferences -> {
                verify(schemas, times(2)).schemaFor(eq(model));
                verify(schema, times(2)).request(eq(model), assertChunkRequest(expectedInput));
                verify(schema, times(2)).response(eq(model), any(), any());
            }))
        );
        verify(client, times(2)).invoke(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testChunkedInfer_noInputs() throws Exception {
        var model = mockModelForChunking();

        SageMakerSchema schema = mock();
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvoke();

        sageMakerService.chunkedInfer(
            model,
            List.of(),
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertOnce(assertNoFailureListener(chunkedInferences -> {
                verify(schemas, never()).schemaFor(any());
                verify(schema, never()).request(any(), any());
                verify(schema, never()).response(any(), any(), any());
            }))
        );
        verify(client, never()).invoke(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private SageMakerModel mockModelForChunking() {
        var model = mockModel();
        when(model.batchSize()).thenReturn(Optional.of(1));
        ModelConfigurations modelConfigurations = mock();
        when(modelConfigurations.getChunkingSettings()).thenReturn(new WordBoundaryChunkingSettings(1, 0));
        when(model.getConfigurations()).thenReturn(modelConfigurations);
        return model;
    }

    private static SageMakerInferenceRequest assertChunkRequest(Set<String> expectedInput) {
        return assertArg(request -> {
            assertThat(request.query(), is(nullValue()));
            assertThat(request.input(), hasSize(1));
            assertThat(request.input().get(0), in(expectedInput));
            assertThat(request.inputType(), equalTo(InputType.UNSPECIFIED));
            assertNull(request.returnDocuments());
            assertNull(request.topN());
            assertFalse(request.stream());
        });
    }

    public void testChunkedInferError() {
        var model = mockModelForChunking();

        var expectedException = new IllegalArgumentException("hola");
        SageMakerSchema schema = mock();
        when(schema.error(any(), any())).thenReturn(expectedException);
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvokeFailure(expectedException);

        var expectedInput = Set.of("first", "second");
        var expectedOutput = Stream.of(expectedException, expectedException).map(ChunkedInferenceError::new).toArray();

        sageMakerService.chunkedInfer(
            model,
            expectedInput.stream().map(ChunkInferenceInput::new).toList(),
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertOnce(assertNoFailureListener(chunkedInferences -> {
                verify(schemas, times(2)).schemaFor(eq(model));
                verify(schema, times(2)).request(eq(model), assertChunkRequest(expectedInput));
                verify(schema, times(2)).error(eq(model), assertArg(e -> assertThat(e, equalTo(expectedException))));
                assertThat(chunkedInferences, containsInAnyOrder(expectedOutput));
            }))
        );
        verify(client, times(2)).invoke(any(), any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testClose() throws IOException {
        sageMakerService.close();
        verify(client, only()).close();
    }

    @Override
    public InferenceService createInferenceService() {
        when(schemas.supportedTaskTypes()).thenReturn(EnumSet.of(TaskType.RERANK, TaskType.TEXT_EMBEDDING, TaskType.COMPLETION));
        return sageMakerService;
    }

    @Override
    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel_UsesDefaultSimilarity() {
        // Coverage for the happy path of this method is handled in SageMakerModelBuilderTests
    }

    @Override
    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel_KeepsSimilarity() {
        // Coverage for the happy path of this method is handled in SageMakerModelBuilderTests
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(
            rerankingInferenceService.rerankerWindowSize("any model"),
            is(RerankingInferenceService.CONSERVATIVE_DEFAULT_WINDOW_SIZE)
        );
    }

    public void testBuildModelFromConfigAndSecrets() {
        var modelConfigurations = mock(ModelConfigurations.class);
        var modelSecrets = mock(ModelSecrets.class);
        sageMakerService.buildModelFromConfigAndSecrets(modelConfigurations, modelSecrets);
        verify(modelBuilder, only()).fromStorage(same(modelConfigurations), same(modelSecrets));
    }
}
