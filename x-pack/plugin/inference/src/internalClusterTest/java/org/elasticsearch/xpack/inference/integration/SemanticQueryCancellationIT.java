/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.AbstractTestInferenceService;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The query-time inference of a semantic query runs as a child of the search task and receives the inference task as its
 * parent, so cancelling the search cancels the inference.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SemanticQueryCancellationIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test_index";
    private static final String FIELD_NAME = "semantic_field";
    private static final String INFERENCE_ID = "sparse_endpoint";

    private final AtomicReference<Runnable> heldInference = new AtomicReference<>();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @Before
    public void setUpEndpointAndIndex() throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(
            client(),
            TaskType.SPARSE_EMBEDDING,
            INFERENCE_ID,
            Map.of("model", "my_model", "api_key", "my_api_key")
        );
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(IntegrationTestUtils.generateSemanticTextMapping(Map.of(FIELD_NAME, INFERENCE_ID)))
        );
    }

    @After
    public void cleanUp() {
        AbstractTestInferenceService.onInfer = (parentTaskId, runInference) -> runInference.run();
        Runnable held = heldInference.getAndSet(null);
        if (held != null) {
            held.run();
        }
        IntegrationTestUtils.deleteIndex(client(), INDEX_NAME);
        IntegrationTestUtils.deleteInferenceEndpoint(client(), TaskType.SPARSE_EMBEDDING, INFERENCE_ID);
    }

    public void testCancellingSearchCancelsQueryTimeInference() throws Exception {
        CountDownLatch inferenceStarted = new CountDownLatch(1);
        AtomicReference<TaskId> inferenceParent = new AtomicReference<>();
        AbstractTestInferenceService.onInfer = (parentTaskId, runInference) -> {
            inferenceParent.set(parentTaskId);
            heldInference.set(runInference);
            inferenceStarted.countDown();
        };

        SearchRequest request = new SearchRequest(INDEX_NAME).source(
            new SearchSourceBuilder().query(new SemanticQueryBuilder(FIELD_NAME, randomAlphaOfLength(10)))
        );
        ActionFuture<SearchResponse> future = client().search(request);
        safeAwait(inferenceStarted);

        TaskInfo searchTask = singleTask(TransportSearchAction.TYPE.name());
        TaskInfo inferenceTask = singleTask(InferenceAction.NAME);
        assertThat(inferenceTask.parentTaskId(), equalTo(searchTask.taskId()));
        assertThat(inferenceParent.get(), equalTo(inferenceTask.taskId()));

        clusterAdmin().prepareCancelTasks().setTargetTaskId(searchTask.taskId()).get();
        assertBusy(() -> assertTrue(singleTask(InferenceAction.NAME).cancelled()));

        heldInference.getAndSet(null).run();
        Exception e = expectThrows(Exception.class, () -> future.actionGet(SAFE_AWAIT_TIMEOUT));
        assertThat(ExceptionsHelper.unwrap(e, TaskCancelledException.class), notNullValue());
    }

    private static TaskInfo singleTask(String action) {
        List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(action).get().getTasks();
        assertThat(tasks, hasSize(1));
        return tasks.get(0);
    }
}
