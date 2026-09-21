/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action.trainedmodel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportCoordinatedInferenceActionTests extends ESTestCase {
    public void testConvertPrefixToInputType_ConvertsIngestCorrectly() {
        assertThat(
            TransportCoordinatedInferenceAction.convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType.INGEST),
            is(InputType.INTERNAL_INGEST)
        );
    }

    public void testConvertPrefixToInputType_ConvertsSearchCorrectly() {
        assertThat(
            TransportCoordinatedInferenceAction.convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType.SEARCH),
            is(InputType.INTERNAL_SEARCH)
        );
    }

    public void testConvertPrefixToInputType_DefaultsToIngestWhenUnknown() {
        assertThat(
            TransportCoordinatedInferenceAction.convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType.NONE),
            is(InputType.INTERNAL_INGEST)
        );
    }

    public void testTaskIsCancellable() {
        var request = CoordinatedInferenceAction.Request.forTextInput("model", List.of("text"), null, false, null);
        var task = request.createTask(
            randomNonNegativeLong(),
            "transport",
            CoordinatedInferenceAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        assertThat(task, instanceOf(CancellableTask.class));
    }

    /**
     * The inference requests must be child tasks of the coordinating task, so that cancelling the caller
     * (a search whose client disconnected, for instance) reaches the queued inference on the ML node.
     */
    public void testInferenceRequestsAreChildrenOfTheCoordinatingTask() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            List<ActionRequest> sentRequests = new ArrayList<>();
            NoOpClient client = new NoOpClient(threadPool) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    sentRequests.add(request);
                }
            };
            var action = new TransportCoordinatedInferenceAction(
                mock(TransportService.class),
                new ActionFilters(Set.of()),
                client,
                clusterService
            );
            var task = new CancellableTask(
                randomNonNegativeLong(),
                "transport",
                CoordinatedInferenceAction.NAME,
                "",
                TaskId.EMPTY_TASK_ID,
                Map.of()
            );

            // no model is deployed in the cluster, so text input goes to the inference service
            action.doExecute(
                task,
                CoordinatedInferenceAction.Request.forTextInput("model", List.of("text"), null, false, null),
                ActionListener.noop()
            );
            // document input always goes to the in-cluster model
            action.doExecute(
                task,
                CoordinatedInferenceAction.Request.forMapInput(
                    "model",
                    List.of(Map.of("field", "value")),
                    null,
                    false,
                    null,
                    CoordinatedInferenceAction.Request.RequestModelType.UNKNOWN
                ),
                ActionListener.noop()
            );

            assertThat(sentRequests, hasSize(2));
            assertThat(sentRequests.get(0), instanceOf(InferenceAction.Request.class));
            assertThat(sentRequests.get(1), instanceOf(InferModelAction.Request.class));
            TaskId expectedParent = new TaskId(clusterService.localNode().getId(), task.getId());
            for (ActionRequest request : sentRequests) {
                assertThat(request.getParentTask(), equalTo(expectedParent));
            }
        } finally {
            terminate(threadPool);
        }
    }
}
