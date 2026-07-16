/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.custom.CustomModelTests;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceParameterizedTestConfiguration;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ModelValidatorBuilderTests extends ESTestCase {

    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clientManager.close();
        terminate(threadPool);
    }

    public void testCustomServiceRerankValidator() {
        var service = CustomServiceParameterizedTestConfiguration.createService(threadPool, clientManager);
        var validator = ModelValidatorBuilder.buildModelValidator(TaskType.RERANK, service);
        var mockService = mock(InferenceService.class);
        validator.validate(
            mockService,
            CustomModelTests.getTestModel(TaskType.RERANK, new RerankResponseParser("score")),
            null,
            ActionListener.noop()
        );

        verify(mockService, times(1)).rerankInfer(
            any(),
            eq(new RerankRequest(InferenceString.fromStringList(List.of("how big")), InferenceString.ofText("test query"), 1, true, null)),
            any(),
            any()
        );
        verifyNoMoreInteractions(mockService);
    }

    public void testBuildModelValidator_NullTaskType() {
        assertThrows(IllegalArgumentException.class, () -> ModelValidatorBuilder.buildModelValidator(null, mock()));
    }

    public void testBuildModelValidator_NullService() {
        assertThrows(IllegalArgumentException.class, () -> ModelValidatorBuilder.buildModelValidator(randomFrom(TaskType.values()), null));
    }

    public void testBuildModelValidator_ValidTaskType() {
        taskTypeToModelValidatorClassMap().forEach(
            (taskType, modelValidatorClass) -> assertThat(
                ModelValidatorBuilder.buildModelValidator(taskType, mock()),
                isA(modelValidatorClass)
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testBuildModelValidator_Rerank_CallsAppropriateRerankMethod() {
        var mockService = mock(InferenceService.class);
        // The test explicitly expects that we don't call infer() but mock the return to prevent the listener from hanging if we do
        doAnswer(ans -> {
            ((ActionListener<InferenceServiceResults>) ans.getArgument(6)).onResponse(mock());
            return null;
        }).when(mockService).infer(any(), any(), anyBoolean(), any(), any(), any(), any());

        doAnswer(ans -> {
            ((ActionListener<InferenceServiceResults>) ans.getArgument(3)).onResponse(mock());
            return null;
        }).when(mockService).rerankInfer(any(), any(), any(), any());

        var modelValidator = ModelValidatorBuilder.buildModelValidator(TaskType.RERANK, mockService);

        var rerankModel = mock(Model.class);
        when(rerankModel.getTaskType()).thenReturn(TaskType.RERANK);

        var timeout = randomPositiveTimeValue();
        var listener = new TestPlainActionFuture<Model>();
        modelValidator.validate(mockService, rerankModel, timeout, listener);
        listener.actionGet(TEST_REQUEST_TIMEOUT);

        verify(mockService).rerankInfer(any(), any(), any(), any());
        verify(mockService, never()).infer(any(), any(), anyBoolean(), any(), any(), any(), any());
    }

    private Map<TaskType, Class<? extends ModelValidator>> taskTypeToModelValidatorClassMap() {
        return Map.of(
            TaskType.TEXT_EMBEDDING,
            DenseEmbeddingModelValidator.class,
            TaskType.SPARSE_EMBEDDING,
            SimpleModelValidator.class,
            TaskType.RERANK,
            SimpleModelValidator.class,
            TaskType.COMPLETION,
            ChatCompletionModelValidator.class,
            TaskType.CHAT_COMPLETION,
            ChatCompletionModelValidator.class,
            TaskType.ANY,
            SimpleModelValidator.class
        );
    }
}
