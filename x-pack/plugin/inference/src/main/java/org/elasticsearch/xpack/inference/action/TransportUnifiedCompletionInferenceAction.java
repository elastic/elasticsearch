/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

import java.util.concurrent.Flow;

public class TransportUnifiedCompletionInferenceAction extends BaseTransportInferenceAction<UnifiedCompletionAction.Request> {

    @Inject
    public TransportUnifiedCompletionInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        super(
            UnifiedCompletionAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            UnifiedCompletionAction.Request::new,
            nodeClient,
            threadPool
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(UnifiedCompletionAction.Request request, Model model) {
        return request.getTaskType().isAnyOrSame(TaskType.CHAT_COMPLETION) == false || model.getTaskType() != TaskType.CHAT_COMPLETION;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(UnifiedCompletionAction.Request request, Model model) {
        return new ElasticsearchStatusException(
            "Incompatible task_type for unified API, the requested type [{}] must be one of [{}]",
            RestStatus.BAD_REQUEST,
            request.getTaskType(),
            TaskType.CHAT_COMPLETION.toString()
        );
    }

    @Override
    protected void doInference(
        Model model,
        UnifiedCompletionAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        service.unifiedCompletionInfer(model, request.getUnifiedCompletionRequest(), request.getTimeout(), listener);
    }

    @Override
    protected void doExecute(Task task, UnifiedCompletionAction.Request request, ActionListener<InferenceAction.Response> listener) {
        super.doExecute(task, request, listener.delegateResponse((l, e) -> l.onFailure(UnifiedChatCompletionException.fromThrowable(e))));
    }

    /**
     * If we get any errors, either in {@link #doExecute} via the listener.onFailure or while streaming, make sure that they are formatted
     * as {@link UnifiedChatCompletionException}.
     */
    @Override
    protected <T> Flow.Publisher<T> streamErrorHandler(Flow.Publisher<T> upstream) {
        return downstream -> {
            upstream.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    downstream.onSubscribe(subscription);
                }

                @Override
                public void onNext(T item) {
                    downstream.onNext(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    downstream.onError(UnifiedChatCompletionException.fromThrowable(throwable));
                }

                @Override
                public void onComplete() {
                    downstream.onComplete();
                }
            });
        };
    }
}
