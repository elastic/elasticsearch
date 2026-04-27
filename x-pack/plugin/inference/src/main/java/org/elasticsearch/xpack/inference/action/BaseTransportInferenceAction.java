/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferenceLicenceCheck;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceTimer;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.inference.telemetry.InferenceStats.responseAttributes;

/**
 * Base class for transport actions that handle inference requests.
 * @param <Request> The specific type of inference request being handled
 */
public abstract class BaseTransportInferenceAction<Request extends BaseInferenceActionRequest> extends HandledTransportAction<
    Request,
    InferenceAction.Response> {

    private static final Logger log = LogManager.getLogger(BaseTransportInferenceAction.class);
    private static final String STREAMING_INFERENCE_TASK_TYPE = "streaming_inference";
    private static final String STREAMING_TASK_ACTION = "xpack/inference/streaming_inference[n]";
    private final XPackLicenseState licenseState;
    private final InferenceEndpointRegistry endpointRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final InferenceStats inferenceStats;
    private final StreamingTaskManager streamingTaskManager;
    private final ThreadPool threadPool;

    public BaseTransportInferenceAction(
        String inferenceActionName,
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        InferenceEndpointRegistry endpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        Writeable.Reader<Request> requestReader,
        ThreadPool threadPool
    ) {
        super(inferenceActionName, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.licenseState = licenseState;
        this.endpointRegistry = endpointRegistry;
        this.serviceRegistry = serviceRegistry;
        this.inferenceStats = inferenceStats;
        this.streamingTaskManager = streamingTaskManager;
        this.threadPool = threadPool;
    }

    protected abstract boolean isInvalidTaskTypeForInferenceEndpoint(Request request, Model model);

    protected abstract ElasticsearchStatusException createInvalidTaskTypeException(Request request, Model model);

    protected abstract void doInference(
        Model model,
        Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    );

    @Override
    protected void doExecute(Task task, Request request, ActionListener<InferenceAction.Response> listener) {
        var timer = InferenceTimer.start();

        var getModelListener = ActionListener.wrap((Model model) -> {
            var serviceName = model.getConfigurations().getService();

            if (InferenceLicenceCheck.isServiceLicenced(serviceName, licenseState) == false) {
                listener.onFailure(InferenceLicenceCheck.complianceException(serviceName));
                return;
            }

            try {
                validateRequest(request, model);
            } catch (Exception e) {
                recordRequestDurationMetrics(model, timer, e);
                listener.onFailure(e);
                return;
            }

            // TODO: this is a temporary solution for passing around the product use case.
            // We want to pass InferenceContext through the various infer methods in InferenceService in the long term
            var context = request.getContext();
            if (Objects.nonNull(context)) {
                var headerNotPresentInThreadContext = Objects.isNull(
                    threadPool.getThreadContext().getHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER)
                );
                if (headerNotPresentInThreadContext) {
                    threadPool.getThreadContext()
                        .putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, context.productUseCase());
                }
            }

            var service = serviceRegistry.getService(serviceName).get();
            inferOnServiceWithMetrics(model, request, service, timer, listener);

        }, e -> {
            try {
                inferenceStats.inferenceDuration()
                    .record(timer.elapsedMillis(), inferenceStats.withConstantAttributes(responseAttributes(e)));
            } catch (Exception metricsException) {
                log.atDebug().withThrowable(metricsException).log("Failed to record metrics when the model is missing, dropping metrics");
            }
            listener.onFailure(e);
        });

        endpointRegistry.getEndpoint(request.getInferenceEntityId(), getModelListener);
    }

    private void validateRequest(Request request, Model model) {
        var serviceName = model.getConfigurations().getService();
        var requestTaskType = request.getTaskType();
        var service = serviceRegistry.getService(serviceName);

        validationHelper(service::isEmpty, () -> unknownServiceException(serviceName, request.getInferenceEntityId()));
        validationHelper(
            () -> request.getTaskType().isAnyOrSame(model.getTaskType()) == false,
            () -> requestModelTaskTypeMismatchException(requestTaskType, model.getTaskType())
        );
        validationHelper(() -> isInvalidTaskTypeForInferenceEndpoint(request, model), () -> createInvalidTaskTypeException(request, model));
    }

    private static void validationHelper(Supplier<Boolean> validationFailure, Supplier<ElasticsearchStatusException> exceptionCreator) {
        if (validationFailure.get()) {
            throw exceptionCreator.get();
        }
    }

    private void recordRequestDurationMetrics(Model model, InferenceTimer timer, @Nullable Throwable t) {
        inferenceStats.inferenceDuration()
            .record(timer.elapsedMillis(), inferenceStats.serviceAndResponseAttributes(model, unwrapCause(t)));
    }

    private void inferOnServiceWithMetrics(
        Model model,
        Request request,
        InferenceService service,
        InferenceTimer timer,
        ActionListener<InferenceAction.Response> listener
    ) {
        recordRequestCountMetrics(model);
        inferOnService(model, request, service, ActionListener.wrap(inferenceResults -> {
            if (request.isStreaming()) {
                var taskProcessor = streamingTaskManager.<InferenceServiceResults.Result>create(
                    STREAMING_INFERENCE_TASK_TYPE,
                    STREAMING_TASK_ACTION
                );
                inferenceResults.publisher().subscribe(taskProcessor);

                var instrumentedStream = publisherWithMetrics(timer, model, taskProcessor);

                var streamErrorHandler = streamErrorHandler(instrumentedStream);

                listener.onResponse(new InferenceAction.Response(inferenceResults, streamErrorHandler));
            } else {
                recordRequestDurationMetrics(model, timer, null);
                listener.onResponse(new InferenceAction.Response(inferenceResults));
            }
        }, e -> {
            recordRequestDurationMetrics(model, timer, e);
            listener.onFailure(e);
        }));
    }

    private <T> Flow.Publisher<T> publisherWithMetrics(InferenceTimer timer, Model model, Flow.Processor<T, T> upstream) {
        return downstream -> {
            upstream.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    downstream.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long n) {
                            subscription.request(n);
                        }

                        @Override
                        public void cancel() {
                            recordRequestDurationMetrics(model, timer, null);
                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(T item) {
                    downstream.onNext(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    recordRequestDurationMetrics(model, timer, throwable);
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    recordRequestDurationMetrics(model, timer, null);
                    downstream.onComplete();
                }
            });
        };
    }

    protected <T> Flow.Publisher<T> streamErrorHandler(Flow.Publisher<T> upstream) {
        return upstream;
    }

    private void recordRequestCountMetrics(Model model) {
        inferenceStats.requestCount().incrementBy(1, inferenceStats.serviceAttributes(model));
    }

    private void inferOnService(Model model, Request request, InferenceService service, ActionListener<InferenceServiceResults> listener) {
        if (request.isStreaming() == false || service.canStream(model.getTaskType())) {
            doInference(model, request, service, listener);
        } else {
            listener.onFailure(unsupportedStreamingTaskException(request, service));
        }
    }

    private ElasticsearchStatusException unsupportedStreamingTaskException(Request request, InferenceService service) {
        var supportedTasks = service.supportedStreamingTasks();
        if (supportedTasks.isEmpty()) {
            return new ElasticsearchStatusException(
                format("Streaming is not allowed for service [%s].", service.name()),
                RestStatus.METHOD_NOT_ALLOWED
            );
        } else {
            var validTasks = supportedTasks.stream().map(TaskType::toString).collect(Collectors.joining(","));
            return new ElasticsearchStatusException(
                format(
                    "Streaming is not allowed for service [%s] and task [%s]. Supported tasks: [%s]",
                    service.name(),
                    request.getTaskType(),
                    validTasks
                ),
                RestStatus.METHOD_NOT_ALLOWED
            );
        }
    }

    private static ElasticsearchStatusException unknownServiceException(String service, String inferenceId) {
        return new ElasticsearchStatusException("Unknown service [{}] for model [{}]", RestStatus.BAD_REQUEST, service, inferenceId);
    }

    private static ElasticsearchStatusException requestModelTaskTypeMismatchException(TaskType requested, TaskType expected) {
        return new ElasticsearchStatusException(
            "Incompatible task_type, the requested type [{}] does not match the model type [{}]",
            RestStatus.BAD_REQUEST,
            requested,
            expected
        );
    }

    private record NodeRoutingDecision(boolean currentNodeShouldHandleRequest, DiscoveryNode targetNode) {
        static NodeRoutingDecision handleLocally() {
            return new NodeRoutingDecision(true, null);
        }

        static NodeRoutingDecision routeTo(DiscoveryNode node) {
            return new NodeRoutingDecision(false, node);
        }
    }
}
