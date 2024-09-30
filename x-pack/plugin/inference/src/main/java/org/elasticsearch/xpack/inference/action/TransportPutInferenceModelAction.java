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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlPlatformArchitecturesUtil;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class TransportPutInferenceModelAction extends TransportMasterNodeAction<
    PutInferenceModelAction.Request,
    PutInferenceModelAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutInferenceModelAction.class);

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final Client client;
    private volatile boolean skipValidationAndStart;

    @Inject
    public TransportPutInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        Client client,
        Settings settings
    ) {
        super(
            PutInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutInferenceModelAction.Request::new,
            indexNameExpressionResolver,
            PutInferenceModelAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.client = client;
        this.skipValidationAndStart = InferencePlugin.SKIP_VALIDATE_AND_START.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(InferencePlugin.SKIP_VALIDATE_AND_START, this::setSkipValidationAndStart);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutInferenceModelAction.Request request,
        ClusterState state,
        ActionListener<PutInferenceModelAction.Response> listener
    ) throws Exception {
        var requestAsMap = requestToMap(request);
        var resolvedTaskType = resolveTaskType(request.getTaskType(), (String) requestAsMap.remove(TaskType.NAME));

        String serviceName = (String) requestAsMap.remove(ModelConfigurations.SERVICE);
        if (serviceName == null) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Inference endpoint configuration is missing the [" + ModelConfigurations.SERVICE + "] setting",
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        var service = serviceRegistry.getService(serviceName);
        if (service.isEmpty()) {
            listener.onFailure(new ElasticsearchStatusException("Unknown service [{}]", RestStatus.BAD_REQUEST, serviceName));
            return;
        }

        // Check if all the nodes in this cluster know about the service
        if (service.get().getMinimalSupportedVersion().after(state.getMinTransportVersion())) {
            logger.warn(
                format(
                    "Service [%s] requires version [%s] but minimum cluster version is [%s]",
                    serviceName,
                    service.get().getMinimalSupportedVersion(),
                    state.getMinTransportVersion()
                )
            );

            listener.onFailure(
                new ElasticsearchStatusException(
                    format(
                        "All nodes in the cluster are not aware of the service [%s]."
                            + "Wait for the cluster to finish upgrading and try again.",
                        serviceName
                    ),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        var assignments = TrainedModelAssignmentUtils.modelAssignments(request.getInferenceEntityId(), clusterService.state());
        if ((assignments == null || assignments.isEmpty()) == false) {
            listener.onFailure(
                ExceptionsHelper.badRequestException(
                    Messages.MODEL_ID_MATCHES_EXISTING_MODEL_IDS_BUT_MUST_NOT,
                    request.getInferenceEntityId()
                )
            );
            return;
        }

        if (service.get().isInClusterService()) {
            // Find the cluster platform as the service may need that
            // information when creating the model
            MlPlatformArchitecturesUtil.getMlNodesArchitecturesSet(listener.delegateFailureAndWrap((delegate, architectures) -> {
                if (architectures.isEmpty() && clusterIsInElasticCloud(clusterService.getClusterSettings())) {
                    parseAndStoreModel(
                        service.get(),
                        request.getInferenceEntityId(),
                        resolvedTaskType,
                        requestAsMap,
                        // In Elastic cloud ml nodes run on Linux x86
                        Set.of("linux-x86_64"),
                        delegate
                    );
                } else {
                    // The architecture field could be an empty set, the individual services will need to handle that
                    parseAndStoreModel(
                        service.get(),
                        request.getInferenceEntityId(),
                        resolvedTaskType,
                        requestAsMap,
                        architectures,
                        delegate
                    );
                }
            }), client, threadPool.executor(InferencePlugin.UTILITY_THREAD_POOL_NAME));
        } else {
            // Not an in cluster service, it does not care about the cluster platform
            parseAndStoreModel(service.get(), request.getInferenceEntityId(), resolvedTaskType, requestAsMap, Set.of(), listener);
        }
    }

    private void parseAndStoreModel(
        InferenceService service,
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<PutInferenceModelAction.Response> listener
    ) {
        ActionListener<Model> storeModelListener = listener.delegateFailureAndWrap(
            (delegate, verifiedModel) -> modelRegistry.storeModel(
                verifiedModel,
                ActionListener.wrap(r -> putAndStartModel(service, verifiedModel, delegate), e -> {
                    if (e.getCause() instanceof StrictDynamicMappingException && e.getCause().getMessage().contains("chunking_settings")) {
                        delegate.onFailure(
                            new ElasticsearchStatusException(
                                "One or more nodes in your cluster does not support chunking_settings. "
                                    + "Please update all nodes in your cluster to the latest version to use chunking_settings.",
                                RestStatus.BAD_REQUEST
                            )
                        );
                    } else {
                        delegate.onFailure(e);
                    }
                })
            )
        );

        ActionListener<Model> parsedModelListener = listener.delegateFailureAndWrap((delegate, model) -> {
            if (skipValidationAndStart) {
                storeModelListener.onResponse(model);
            } else {
                service.checkModelConfig(model, storeModelListener);
            }
        });

        service.parseRequestConfig(inferenceEntityId, taskType, config, platformArchitectures, parsedModelListener);

    }

    private void putAndStartModel(InferenceService service, Model model, ActionListener<PutInferenceModelAction.Response> finalListener) {
        SubscribableListener.<Boolean>newForked(listener -> {
            var errorCatchingListener = ActionListener.<Boolean>wrap(listener::onResponse, e -> { listener.onResponse(false); });
            service.isModelDownloaded(model, errorCatchingListener);
        }).<Boolean>andThen((listener, isDownloaded) -> {
            if (isDownloaded == false) {
                service.putModel(model, listener);
            } else {
                listener.onResponse(true);
            }
        }).<PutInferenceModelAction.Response>andThen((listener, modelDidPut) -> {
            if (modelDidPut) {
                if (skipValidationAndStart) {
                    listener.onResponse(new PutInferenceModelAction.Response(model.getConfigurations()));
                } else {
                    service.start(
                        model,
                        listener.delegateFailureAndWrap(
                            (l3, ok) -> l3.onResponse(new PutInferenceModelAction.Response(model.getConfigurations()))
                        )
                    );
                }
            } else {
                logger.warn("Failed to put model [{}]", model.getInferenceEntityId());
            }
        }).addListener(finalListener);
    }

    private Map<String, Object> requestToMap(PutInferenceModelAction.Request request) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                request.getContent(),
                request.getContentType()
            )
        ) {
            return parser.map();
        }
    }

    private void setSkipValidationAndStart(boolean skipValidationAndStart) {
        this.skipValidationAndStart = skipValidationAndStart;
    }

    @Override
    protected ClusterBlockException checkBlock(PutInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    static boolean clusterIsInElasticCloud(ClusterSettings settings) {
        // use a heuristic to determine if in Elastic cloud.
        // One such heuristic is where USE_AUTO_MACHINE_MEMORY_PERCENT == true
        return settings.get(MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT);
    }

    /**
     * task_type can be specified as either a URL parameter or in the
     * request body. Resolve which to use or throw if the settings are
     * inconsistent
     * @param urlTaskType Taken from the URL parameter. ANY means not specified.
     * @param bodyTaskType Taken from the request body. Maybe null
     * @return The resolved task type
     */
    static TaskType resolveTaskType(TaskType urlTaskType, String bodyTaskType) {
        if (bodyTaskType == null) {
            if (urlTaskType == TaskType.ANY) {
                throw new ElasticsearchStatusException("model is missing required setting [task_type]", RestStatus.BAD_REQUEST);
            } else {
                return urlTaskType;
            }
        }

        TaskType parsedBodyTask = TaskType.fromStringOrStatusException(bodyTaskType);
        if (parsedBodyTask == TaskType.ANY) {
            throw new ElasticsearchStatusException("task_type [any] is not valid type for inference", RestStatus.BAD_REQUEST);
        }

        if (parsedBodyTask.isAnyOrSame(urlTaskType) == false) {
            throw new ElasticsearchStatusException(
                "Cannot resolve conflicting task_type parameter in the request URL [{}] and the request body [{}]",
                RestStatus.BAD_REQUEST,
                urlTaskType.toString(),
                bodyTaskType
            );
        }

        return parsedBodyTask;
    }
}
