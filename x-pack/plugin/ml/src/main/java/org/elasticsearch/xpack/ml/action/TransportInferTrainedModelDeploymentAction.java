/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.deployment.NlpInferenceInput;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportInferTrainedModelDeploymentAction extends TransportTasksAction<
    TrainedModelDeploymentTask,
    InferTrainedModelDeploymentAction.Request,
    InferTrainedModelDeploymentAction.Response,
    InferTrainedModelDeploymentAction.Response> {

    @Inject
    public TransportInferTrainedModelDeploymentAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            InferTrainedModelDeploymentAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            InferTrainedModelDeploymentAction.Request::new,
            InferTrainedModelDeploymentAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected InferTrainedModelDeploymentAction.Response newResponse(
        InferTrainedModelDeploymentAction.Request request,
        List<InferTrainedModelDeploymentAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (taskOperationFailures.isEmpty() == false) {
            throw ExceptionsHelper.taskOperationFailureToStatusException(taskOperationFailures.get(0));
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw failedNodeExceptions.get(0);
        } else if (tasks.isEmpty()) {
            throw new ElasticsearchStatusException(
                "Unable to find model deployment task [{}] please stop and start the deployment or try again momentarily",
                RestStatus.NOT_FOUND,
                request.getId()
            );
        } else {
            assert tasks.size() == 1;
            return tasks.get(0);
        }
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        InferTrainedModelDeploymentAction.Request request,
        TrainedModelDeploymentTask task,
        ActionListener<InferTrainedModelDeploymentAction.Response> listener
    ) {
        var nlpInputs = new ArrayList<NlpInferenceInput>();
        if (request.getTextInput() != null) {
            for (var text : request.getTextInput()) {
                nlpInputs.add(NlpInferenceInput.fromText(text));
            }
        } else {
            for (var doc : request.getDocs()) {
                nlpInputs.add(NlpInferenceInput.fromDoc(doc));
            }
        }

        // Multiple documents to infer on, wait for all results
        // and return order the results to match the request order
        AtomicInteger count = new AtomicInteger();
        AtomicArray<InferenceResults> results = new AtomicArray<>(nlpInputs.size());
        int slot = 0;
        for (var input : nlpInputs) {
            task.infer(
                input,
                request.getUpdate(),
                request.isHighPriority(),
                request.getInferenceTimeout(),
                request.getPrefixType(),
                actionTask,
                request.isChunkResults(),
                orderedListener(count, results, slot++, nlpInputs.size(), listener)
            );
        }
    }

    /**
     * Create a listener that groups the results in the correct order.
     * Exceptions are converted to {@link ErrorInferenceResults},
     * the listener will never call {@code finalListener::onFailure}
     * instead failures are returned as inference results.
     */
    static ActionListener<InferenceResults> orderedListener(
        AtomicInteger count,
        AtomicArray<InferenceResults> results,
        int slot,
        int totalNumberOfResponses,
        ActionListener<InferTrainedModelDeploymentAction.Response> finalListener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(InferenceResults response) {
                results.setOnce(slot, response);
                if (count.incrementAndGet() == totalNumberOfResponses) {
                    sendResponse();
                }
            }

            @Override
            public void onFailure(Exception e) {
                results.setOnce(slot, new ErrorInferenceResults(e));
                if (count.incrementAndGet() == totalNumberOfResponses) {
                    sendResponse();
                }
            }

            private void sendResponse() {
                finalListener.onResponse(new InferTrainedModelDeploymentAction.Response(results.asList()));
            }
        };
    }
}
