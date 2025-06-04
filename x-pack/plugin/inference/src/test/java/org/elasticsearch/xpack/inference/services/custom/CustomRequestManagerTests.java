/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CustomRequestManagerTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testCreateRequest_ThrowsException_ForInvalidUrl() {
        var inferenceId = "inference_id";

        var requestContentString = """
            {
                "input": ${input}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS,
            "${url}",
            null,
            null,
            requestContentString,
            new RerankResponseParser("$.result.score"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.RERANK,
            serviceSettings,
            new CustomTaskSettings(Map.of("url", "^")),
            new CustomSecretSettings(Map.of("api_key", new SecureString("my-secret-key".toCharArray())))
        );

        var listener = new PlainActionFuture<InferenceServiceResults>();
        var manager = CustomRequestManager.of(model, threadPool);
        manager.execute(new EmbeddingsInput(List.of("abc", "123"), null, null), mock(RequestSender.class), () -> false, listener);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.timeValueSeconds(30)));

        assertThat(exception.getMessage(), is("Failed to construct the custom service request"));
        assertThat(exception.getCause().getMessage(), is("Failed to build URI, error: Illegal character in path at index 0: ^"));
    }
}
