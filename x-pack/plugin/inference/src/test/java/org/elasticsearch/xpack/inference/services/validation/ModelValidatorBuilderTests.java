/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.custom.CustomModelTests;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceTests;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ModelValidatorBuilderTests extends ESTestCase {

    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clientManager.close();
        terminate(threadPool);
    }

    public void testCustomServiceValidator() {
        var service = CustomServiceTests.createService(threadPool, clientManager);
        var validator = ModelValidatorBuilder.buildModelValidator(TaskType.RERANK, service);
        var mockService = mock(InferenceService.class);
        validator.validate(
            mockService,
            CustomModelTests.getTestModel(TaskType.RERANK, new RerankResponseParser("score")),
            null,
            ActionListener.noop()
        );

        verify(mockService, times(1)).infer(
            any(),
            eq("test query"),
            eq(true),
            eq(1),
            eq(List.of("how big")),
            eq(false),
            eq(Map.of()),
            eq(InputType.INTERNAL_INGEST),
            any(),
            any()
        );
        verifyNoMoreInteractions(mockService);
    }

    public void testBuildModelValidator_NullTaskType() {
        assertThrows(IllegalArgumentException.class, () -> { ModelValidatorBuilder.buildModelValidator(null, null); });
    }

    public void testBuildModelValidator_ValidTaskType() {
        taskTypeToModelValidatorClassMap().forEach((taskType, modelValidatorClass) -> {
            assertThat(ModelValidatorBuilder.buildModelValidator(taskType, null), isA(modelValidatorClass));
        });
    }

    private Map<TaskType, Class<? extends ModelValidator>> taskTypeToModelValidatorClassMap() {
        return Map.of(
            TaskType.TEXT_EMBEDDING,
            TextEmbeddingModelValidator.class,
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
