/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.common.InferenceServiceNodeLocalRateLimitCalculator;
import org.elasticsearch.xpack.inference.common.InferenceServiceRateLimitCalculator;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;
import org.elasticsearch.xpack.inference.telemetry.InferenceTimer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.exception.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_API_FEATURE;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.modelAttributes;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.responseAttributes;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.routingAttributes;

/**
 * Base class for transport actions that handle inference requests.
 * Works in conjunction with {@link InferenceServiceNodeLocalRateLimitCalculator} to
 * route requests to specific nodes, iff they support "node-local" rate limiting, which is described in detail
 * in {@link InferenceServiceNodeLocalRateLimitCalculator}.
 *
 * @param <Request> The specific type of inference request being handled
 */
public abstract class BaseTransportInferenceAction<Request extends BaseInferenceActionRequest> extends HandledTransportAction<
    Request,
    InferenceAction.Response> {

    private static final Logger log = LogManager.getLogger(BaseTransportInferenceAction.class);
    private static final String STREAMING_INFERENCE_TASK_TYPE = "streaming_inference";
    private static final String STREAMING_TASK_ACTION = "xpack/inference/streaming_inference[n]";
    private final XPackLicenseState licenseState;
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final InferenceStats inferenceStats;
    private final StreamingTaskManager streamingTaskManager;
    private final InferenceServiceRateLimitCalculator inferenceServiceRateLimitCalculator;
    private final NodeClient nodeClient;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final Random random;

    public BaseTransportInferenceAction(
        String inferenceActionName,
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        Writeable.Reader<Request> requestReader,
        InferenceServiceRateLimitCalculator inferenceServiceNodeLocalRateLimitCalculator,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        super(inferenceActionName, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.licenseState = licenseState;
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.inferenceStats = inferenceStats;
        this.streamingTaskManager = streamingTaskManager;
        this.inferenceServiceRateLimitCalculator = inferenceServiceNodeLocalRateLimitCalculator;
        this.nodeClient = nodeClient;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.random = Randomness.get();
    }

    protected abstract boolean isInvalidTaskTypeForInferenceEndpoint(Request request, UnparsedModel unparsedModel);

    protected abstract ElasticsearchStatusException createInvalidTaskTypeException(Request request, UnparsedModel unparsedModel);

    protected abstract void doInference(
        Model model,
        Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    );

    @Override
    protected void doExecute(Task task, Request request, ActionListener<InferenceAction.Response> listener) {
        if (INFERENCE_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.INFERENCE));
            return;
        }

        var timer = InferenceTimer.start();

        var getModelListener = ActionListener.wrap((UnparsedModel unparsedModel) -> {
            var serviceName = unparsedModel.service();

            try {
                validateRequest(request, unparsedModel);
            } catch (Exception e) {
                recordRequestDurationMetrics(unparsedModel, timer, e);
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
            var localNodeId = nodeClient.getLocalNodeId();
            var routingDecision = determineRouting(serviceName, request, unparsedModel, localNodeId);

            if (routingDecision.currentNodeShouldHandleRequest()) {
                var model = service.parsePersistedConfigWithSecrets(
                    unparsedModel.inferenceEntityId(),
                    unparsedModel.taskType(),
                    unparsedModel.settings(),
                    unparsedModel.secrets()
                );
                inferOnServiceWithMetrics(model, request, service, timer, localNodeId, listener);
            } else {
                // Reroute request
                request.setHasBeenRerouted(true);
                rerouteRequest(request, listener, routingDecision.targetNode);
            }
        }, e -> {
            try {
                inferenceStats.inferenceDuration().record(timer.elapsedMillis(), responseAttributes(e));
            } catch (Exception metricsException) {
                log.atDebug().withThrowable(metricsException).log("Failed to record metrics when the model is missing, dropping metrics");
            }
            listener.onFailure(e);
        });

        modelRegistry.getModelWithSecrets(request.getInferenceEntityId(), getModelListener);
    }

    private void validateRequest(Request request, UnparsedModel unparsedModel) {
        var serviceName = unparsedModel.service();
        var requestTaskType = request.getTaskType();
        var service = serviceRegistry.getService(serviceName);

        validationHelper(service::isEmpty, () -> unknownServiceException(serviceName, request.getInferenceEntityId()));
        validationHelper(
            () -> request.getTaskType().isAnyOrSame(unparsedModel.taskType()) == false,
            () -> requestModelTaskTypeMismatchException(requestTaskType, unparsedModel.taskType())
        );
        validationHelper(
            () -> isInvalidTaskTypeForInferenceEndpoint(request, unparsedModel),
            () -> createInvalidTaskTypeException(request, unparsedModel)
        );
    }

    private NodeRoutingDecision determineRouting(String serviceName, Request request, UnparsedModel unparsedModel, String localNodeId) {
        var modelTaskType = unparsedModel.taskType();

        // Rerouting not supported or request was already rerouted
        if (inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceName, modelTaskType) == false
            || request.hasBeenRerouted()) {
            return NodeRoutingDecision.handleLocally();
        }

        var rateLimitAssignment = inferenceServiceRateLimitCalculator.getRateLimitAssignment(serviceName, modelTaskType);

        // No assignment yet
        if (rateLimitAssignment == null) {
            return NodeRoutingDecision.handleLocally();
        }

        var responsibleNodes = rateLimitAssignment.responsibleNodes();

        // Empty assignment
        if (responsibleNodes == null || responsibleNodes.isEmpty()) {
            return NodeRoutingDecision.handleLocally();
        }

        var nodeToHandleRequest = responsibleNodes.get(random.nextInt(responsibleNodes.size()));

        // The drawn node is the current node
        if (nodeToHandleRequest.getId().equals(localNodeId)) {
            return NodeRoutingDecision.handleLocally();
        }

        // Reroute request
        return NodeRoutingDecision.routeTo(nodeToHandleRequest);
    }

    private static void validationHelper(Supplier<Boolean> validationFailure, Supplier<ElasticsearchStatusException> exceptionCreator) {
        if (validationFailure.get()) {
            throw exceptionCreator.get();
        }
    }

    private void rerouteRequest(Request request, ActionListener<InferenceAction.Response> listener, DiscoveryNode nodeToHandleRequest) {
        transportService.sendRequest(
            nodeToHandleRequest,
            InferenceAction.NAME,
            request,
            new TransportResponseHandler<InferenceAction.Response>() {
                @Override
                public Executor executor() {
                    return threadPool.executor(InferencePlugin.UTILITY_THREAD_POOL_NAME);
                }

                @Override
                public void handleResponse(InferenceAction.Response response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public InferenceAction.Response read(StreamInput in) throws IOException {
                    return new InferenceAction.Response(in);
                }
            }
        );
    }

    private void recordRequestDurationMetrics(UnparsedModel model, InferenceTimer timer, @Nullable Throwable t) {
        try {
            Map<String, Object> metricAttributes = new HashMap<>();
            metricAttributes.putAll(modelAttributes(model));
            metricAttributes.putAll(responseAttributes(unwrapCause(t)));

            inferenceStats.inferenceDuration().record(timer.elapsedMillis(), metricAttributes);
        } catch (Exception e) {
            log.atDebug().withThrowable(e).log("Failed to record metrics with an unparsed model, dropping metrics");
        }
    }

    private void inferOnServiceWithMetrics(
        Model model,
        Request request,
        InferenceService service,
        InferenceTimer timer,
        String localNodeId,
        ActionListener<InferenceAction.Response> listener
    ) {
        recordRequestCountMetrics(model, request, localNodeId);
        inferOnService(model, request, service, ActionListener.wrap(inferenceResults -> {
            if (request.isStreaming()) {
                var taskProcessor = streamingTaskManager.<InferenceServiceResults.Result>create(
                    STREAMING_INFERENCE_TASK_TYPE,
                    STREAMING_TASK_ACTION
                );
                inferenceResults.publisher().subscribe(taskProcessor);

                var instrumentedStream = publisherWithMetrics(timer, model, request, localNodeId, taskProcessor);

                var streamErrorHandler = streamErrorHandler(instrumentedStream);

                listener.onResponse(new InferenceAction.Response(inferenceResults, streamErrorHandler));
            } else {
                recordRequestDurationMetrics(model, timer, request, localNodeId, null);
                listener.onResponse(new InferenceAction.Response(inferenceResults));
            }
        }, e -> {
            recordRequestDurationMetrics(model, timer, request, localNodeId, e);
            listener.onFailure(e);
        }));
    }

    private <T> Flow.Publisher<T> publisherWithMetrics(
        InferenceTimer timer,
        Model model,
        Request request,
        String localNodeId,
        Flow.Processor<T, T> upstream
    ) {
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
                            recordRequestDurationMetrics(model, timer, request, localNodeId, null);
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
                    recordRequestDurationMetrics(model, timer, request, localNodeId, throwable);
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    recordRequestDurationMetrics(model, timer, request, localNodeId, null);
                    downstream.onComplete();
                }
            });
        };
    }

    protected <T> Flow.Publisher<T> streamErrorHandler(Flow.Publisher<T> upstream) {
        return upstream;
    }

    private void recordRequestCountMetrics(Model model, Request request, String localNodeId) {
        Map<String, Object> requestCountAttributes = new HashMap<>();
        requestCountAttributes.putAll(modelAttributes(model));
        requestCountAttributes.putAll(routingAttributes(request, localNodeId));

        inferenceStats.requestCount().incrementBy(1, requestCountAttributes);
    }

    private void recordRequestDurationMetrics(
        Model model,
        InferenceTimer timer,
        Request request,
        String localNodeId,
        @Nullable Throwable t
    ) {
        try {
            Map<String, Object> metricAttributes = new HashMap<>();
            metricAttributes.putAll(modelAttributes(model));
            metricAttributes.putAll(routingAttributes(request, localNodeId));
            metricAttributes.putAll(responseAttributes(unwrapCause(t)));

            inferenceStats.inferenceDuration().record(timer.elapsedMillis(), metricAttributes);
        } catch (Exception e) {
            log.atDebug().withThrowable(e).log("Failed to record metrics with a parsed model, dropping metrics");
        }
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
