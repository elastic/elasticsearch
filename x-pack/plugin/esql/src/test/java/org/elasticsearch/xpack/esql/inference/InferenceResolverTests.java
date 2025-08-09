/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceResolverTests extends ESTestCase {
    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
                between(1, 10),
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testCollectInferenceIds() {
        // Rerank inference plan
        assertCollectInferenceIds(
            "FROM books METADATA _score | RERANK \"italian food recipe\" ON title WITH { \"inference_id\": \"rerank-inference-id\" }",
            List.of("rerank-inference-id")
        );

        // Completion inference plan
        assertCollectInferenceIds(
            "FROM books METADATA _score | COMPLETION \"italian food recipe\" WITH { \"inference_id\": \"completion-inference-id\" }",
            List.of("completion-inference-id")
        );

        // Multiple inference plans
        assertCollectInferenceIds("""
            FROM books METADATA _score
            | RERANK "italian food recipe" ON title WITH { "inference_id": "rerank-inference-id" }
            | COMPLETION "italian food recipe" WITH { "inference_id": "completion-inference-id" }
            """, List.of("rerank-inference-id", "completion-inference-id"));

        // No inference operations
        assertCollectInferenceIds("FROM books | WHERE title:\"test\"", List.of());
    }

    private void assertCollectInferenceIds(String query, List<String> expectedInferenceIds) {
        Set<String> inferenceIds = new HashSet<>();
        InferenceResolver inferenceResolver = inferenceResolver();
        inferenceResolver.collectInferenceIds(new EsqlParser().createStatement(query, configuration(query)), inferenceIds::add);
        assertThat(inferenceIds, containsInAnyOrder(expectedInferenceIds.toArray(new String[0])));
    }

    public void testResolveInferenceIds() throws Exception {
        InferenceResolver inferenceResolver = inferenceResolver();
        List<String> inferenceIds = List.of("rerank-plan");
        SetOnce<InferenceResolution> inferenceResolutionSetOnce = new SetOnce<>();

        inferenceResolver.resolveInferenceIds(inferenceIds, ActionListener.wrap(inferenceResolutionSetOnce::set, e -> {
            throw new RuntimeException(e);
        }));

        assertBusy(() -> {
            InferenceResolution inferenceResolution = inferenceResolutionSetOnce.get();
            assertNotNull(inferenceResolution);
            assertThat(inferenceResolution.resolvedInferences(), contains(new ResolvedInference("rerank-plan", TaskType.RERANK)));
            assertThat(inferenceResolution.hasError(), equalTo(false));
        });
    }

    public void testResolveMultipleInferenceIds() throws Exception {
        InferenceResolver inferenceResolver = inferenceResolver();
        List<String> inferenceIds = List.of("rerank-plan", "rerank-plan", "completion-plan");
        SetOnce<InferenceResolution> inferenceResolutionSetOnce = new SetOnce<>();

        inferenceResolver.resolveInferenceIds(inferenceIds, ActionListener.wrap(inferenceResolutionSetOnce::set, e -> {
            throw new RuntimeException(e);
        }));

        assertBusy(() -> {
            InferenceResolution inferenceResolution = inferenceResolutionSetOnce.get();
            assertNotNull(inferenceResolution);

            assertThat(
                inferenceResolution.resolvedInferences(),
                contains(
                    new ResolvedInference("rerank-plan", TaskType.RERANK),
                    new ResolvedInference("completion-plan", TaskType.COMPLETION)
                )
            );
            assertThat(inferenceResolution.hasError(), equalTo(false));
        });
    }

    public void testResolveMissingInferenceIds() throws Exception {
        InferenceResolver inferenceResolver = inferenceResolver();
        List<String> inferenceIds = List.of("missing-plan");

        SetOnce<InferenceResolution> inferenceResolutionSetOnce = new SetOnce<>();

        inferenceResolver.resolveInferenceIds(inferenceIds, ActionListener.wrap(inferenceResolutionSetOnce::set, e -> {
            throw new RuntimeException(e);
        }));

        assertBusy(() -> {
            InferenceResolution inferenceResolution = inferenceResolutionSetOnce.get();
            assertNotNull(inferenceResolution);

            assertThat(inferenceResolution.resolvedInferences(), empty());
            assertThat(inferenceResolution.hasError(), equalTo(true));
            assertThat(inferenceResolution.getError("missing-plan"), equalTo("inference endpoint not found"));
        });
    }

    @SuppressWarnings({ "unchecked", "raw-types" })
    private Client mockClient() {
        Client client = mock(Client.class);
        doAnswer(i -> {
            Runnable sendResponse = () -> {
                GetInferenceModelAction.Request request = i.getArgument(1, GetInferenceModelAction.Request.class);
                ActionListener<ActionResponse> listener = (ActionListener<ActionResponse>) i.getArgument(2, ActionListener.class);
                ActionResponse response = getInferenceModelResponse(request);

                if (response == null) {
                    listener.onFailure(new ResourceNotFoundException("inference endpoint not found"));
                } else {
                    listener.onResponse(response);
                }
            };

            if (randomBoolean()) {
                sendResponse.run();
            } else {
                threadPool.schedule(
                    sendResponse,
                    TimeValue.timeValueNanos(between(1, 1_000)),
                    threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME)
                );
            }

            return null;
        }).when(client).execute(eq(GetInferenceModelAction.INSTANCE), any(), any());
        return client;
    }

    private static ActionResponse getInferenceModelResponse(GetInferenceModelAction.Request request) {
        GetInferenceModelAction.Response response = mock(GetInferenceModelAction.Response.class);

        if (request.getInferenceEntityId().equals("rerank-plan")) {
            when(response.getEndpoints()).thenReturn(List.of(mockModelConfig("rerank-plan", TaskType.RERANK)));
            return response;
        }

        if (request.getInferenceEntityId().equals("completion-plan")) {
            when(response.getEndpoints()).thenReturn(List.of(mockModelConfig("completion-plan", TaskType.COMPLETION)));
            return response;
        }

        return null;
    }

    private InferenceResolver inferenceResolver() {
        return new InferenceResolver(mockClient());
    }

    private static ModelConfigurations mockModelConfig(String inferenceId, TaskType taskType) {
        return new ModelConfigurations(inferenceId, taskType, randomIdentifier(), mock(ServiceSettings.class));
    }
}
