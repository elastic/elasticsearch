/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponse;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModelBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStreamSchema;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.action.ActionListener.assertOnce;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.core.TimeValue.THIRTY_SECONDS;
import static org.elasticsearch.xpack.core.inference.action.UnifiedCompletionRequestTests.randomUnifiedCompletionRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SageMakerServiceTests extends ESTestCase {

    private static final String QUERY = "query";
    private static final List<String> INPUT = List.of("input");
    private static final InputType INPUT_TYPE = InputType.UNSPECIFIED;

    private SageMakerModelBuilder modelBuilder;
    private SageMakerClient client;
    private SageMakerSchemas schemas;
    private SageMakerService sageMakerService;

    @Before
    public void init() {
        modelBuilder = mock();
        client = mock();
        schemas = mock();
        ThreadPool threadPool = mock();
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        sageMakerService = new SageMakerService(modelBuilder, client, schemas, threadPool, Map::of);
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

    public void testParsePersistedConfigWithSecrets() {
        sageMakerService.parsePersistedConfigWithSecrets("modelId", TaskType.ANY, Map.of(), Map.of());
        verify(modelBuilder, only()).fromStorage(eq("modelId"), eq(TaskType.ANY), eq(SageMakerService.NAME), eq(Map.of()), eq(Map.of()));
    }

    public void testParsePersistedConfig() {
        sageMakerService.parsePersistedConfig("modelId", TaskType.ANY, Map.of());
        verify(modelBuilder, only()).fromStorage(eq("modelId"), eq(TaskType.ANY), eq(SageMakerService.NAME), eq(Map.of()), eq(null));
    }

    public void testInferWithWrongModel() {
        sageMakerService.infer(
            mockUnsupportedModel(),
            QUERY,
            false,
            null,
            INPUT,
            false,
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertUnsupportedModel()
        );
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

        sageMakerService.infer(
            model,
            QUERY,
            null,
            null,
            INPUT,
            false,
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertNoFailureListener(ignored -> {
                verify(schemas, only()).schemaFor(eq(model));
                verify(schema, times(1)).request(eq(model), assertRequest());
                verify(schema, times(1)).response(eq(model), any(), any());
            })
        );
        verify(client, only()).invoke(any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private SageMakerModel mockModel() {
        SageMakerModel model = mock();
        when(model.override(null)).thenReturn(model);
        when(model.awsSecretSettings()).thenReturn(
            Optional.of(new AwsSecretSettings(new SecureString("test-accessKey"), new SecureString("test-secretKey")))
        );
        return model;
    }

    private void mockInvoke() {
        doAnswer(ans -> {
            ActionListener<InvokeEndpointResponse> responseListener = ans.getArgument(3);
            responseListener.onResponse(InvokeEndpointResponse.builder().build());
            return null; // Void
        }).when(client).invoke(any(), any(), any(), any());
    }

    private static SageMakerInferenceRequest assertRequest() {
        return assertArg(request -> {
            assertThat(request.query(), equalTo(QUERY));
            assertThat(request.input(), containsInAnyOrder(INPUT.toArray()));
            assertThat(request.inputType(), equalTo(InputType.UNSPECIFIED));
            assertNull(request.returnDocuments());
            assertNull(request.topN());
        });
    }

    public void testInferStream() {
        SageMakerModel model = mockModel();

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        mockInvokeStream();

        sageMakerService.infer(model, QUERY, null, null, INPUT, true, null, INPUT_TYPE, THIRTY_SECONDS, assertNoFailureListener(ignored -> {
            verify(schemas, only()).streamSchemaFor(eq(model));
            verify(schema, times(1)).streamRequest(eq(model), assertRequest());
            verify(schema, times(1)).streamResponse(eq(model), any());
        }));
        verify(client, only()).invokeStream(any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private void mockInvokeStream() {
        doAnswer(ans -> {
            ActionListener<SageMakerClient.SageMakerStream> responseListener = ans.getArgument(3);
            responseListener.onResponse(
                new SageMakerClient.SageMakerStream(InvokeEndpointWithResponseStreamResponse.builder().build(), mock())
            );
            return null; // Void
        }).when(client).invokeStream(any(), any(), any(), any());
    }

    public void testInferError() {
        SageMakerModel model = mockModel();

        var expectedException = new IllegalArgumentException("hola");
        SageMakerSchema schema = mock();
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvokeFailure(expectedException);

        sageMakerService.infer(
            model,
            QUERY,
            null,
            null,
            INPUT,
            false,
            null,
            INPUT_TYPE,
            THIRTY_SECONDS,
            assertNoSuccessListener(ignored -> {
                verify(schemas, only()).schemaFor(eq(model));
                verify(schema, times(1)).request(eq(model), assertRequest());
                verify(schema, times(1)).error(eq(model), assertArg(e -> assertThat(e, equalTo(expectedException))));
            })
        );
        verify(client, only()).invoke(any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private void mockInvokeFailure(Exception e) {
        doAnswer(ans -> {
            ActionListener<?> responseListener = ans.getArgument(3);
            responseListener.onFailure(e);
            return null; // Void
        }).when(client).invoke(any(), any(), any(), any());
    }

    public void testInferException() {
        SageMakerModel model = mockModel();
        when(model.getInferenceEntityId()).thenReturn("some id");

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        doThrow(new IllegalArgumentException("wow, really?")).when(client).invokeStream(any(), any(), any(), any());

        sageMakerService.infer(model, QUERY, null, null, INPUT, true, null, INPUT_TYPE, THIRTY_SECONDS, assertNoSuccessListener(e -> {
            verify(schemas, only()).streamSchemaFor(eq(model));
            verify(schema, times(1)).streamRequest(eq(model), assertRequest());
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(e.getMessage(), equalTo("Failed to call SageMaker for inference id [some id]."));
        }));
        verify(client, only()).invokeStream(any(), any(), any(), any());
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
            randomUnifiedCompletionRequest(),
            THIRTY_SECONDS,
            assertNoFailureListener(ignored -> {
                verify(schemas, only()).streamSchemaFor(eq(model));
                verify(schema, times(1)).chatCompletionStreamRequest(eq(model), any());
                verify(schema, times(1)).chatCompletionStreamResponse(eq(model), any());
            })
        );
        verify(client, only()).invokeStream(any(), any(), any(), any());
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
            randomUnifiedCompletionRequest(),
            THIRTY_SECONDS,
            assertNoSuccessListener(ignored -> {
                verify(schemas, only()).streamSchemaFor(eq(model));
                verify(schema, times(1)).chatCompletionStreamRequest(eq(model), any());
                verify(schema, times(1)).chatCompletionError(eq(model), assertArg(e -> assertThat(e, equalTo(expectedException))));
            })
        );
        verify(client, only()).invokeStream(any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    private void mockInvokeStreamFailure(Exception e) {
        doAnswer(ans -> {
            ActionListener<?> responseListener = ans.getArgument(3);
            responseListener.onFailure(e);
            return null; // Void
        }).when(client).invokeStream(any(), any(), any(), any());
    }

    public void testUnifiedInferException() {
        SageMakerModel model = mockModel();
        when(model.getInferenceEntityId()).thenReturn("some id");

        SageMakerStreamSchema schema = mock();
        when(schemas.streamSchemaFor(model)).thenReturn(schema);
        doThrow(new IllegalArgumentException("wow, rude")).when(client).invokeStream(any(), any(), any(), any());

        sageMakerService.unifiedCompletionInfer(model, randomUnifiedCompletionRequest(), THIRTY_SECONDS, assertNoSuccessListener(e -> {
            verify(schemas, only()).streamSchemaFor(eq(model));
            verify(schema, times(1)).chatCompletionStreamRequest(eq(model), any());
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(e.getMessage(), equalTo("Failed to call SageMaker for inference id [some id]."));
        }));
        verify(client, only()).invokeStream(any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testChunkedInferWithWrongModel() {
        sageMakerService.chunkedInfer(
            mockUnsupportedModel(),
            QUERY,
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
        when(schema.response(any(), any(), any())).thenReturn(TextEmbeddingFloatResultsTests.createRandomResults());
        when(schemas.schemaFor(model)).thenReturn(schema);
        mockInvoke();

        var expectedInput = Set.of("first", "second");

        sageMakerService.chunkedInfer(
            model,
            QUERY,
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
        verify(client, times(2)).invoke(any(), any(), any(), any());
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
            assertThat(request.query(), equalTo(QUERY));
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
            QUERY,
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
        verify(client, times(2)).invoke(any(), any(), any(), any());
        verifyNoMoreInteractions(client, schemas, schema);
    }

    public void testClose() throws IOException {
        sageMakerService.close();
        verify(client, only()).close();
    }

}
