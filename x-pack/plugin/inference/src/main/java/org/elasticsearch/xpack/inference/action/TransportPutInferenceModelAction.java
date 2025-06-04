/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.validation.ModelValidatorBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_API_FEATURE;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME;

public class TransportPutInferenceModelAction extends TransportMasterNodeAction<
    PutInferenceModelAction.Request,
    PutInferenceModelAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutInferenceModelAction.class);

    private final XPackLicenseState licenseState;
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private volatile boolean skipValidationAndStart;

    @Inject
    public TransportPutInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        Settings settings
    ) {
        super(
            PutInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutInferenceModelAction.Request::new,
            PutInferenceModelAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = licenseState;
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
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
        if (INFERENCE_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.INFERENCE));
            return;
        }

        if (modelRegistry.containsDefaultConfigId(request.getInferenceEntityId())) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "[{}] is a reserved inference ID. Cannot create a new inference endpoint with a reserved ID.",
                    RestStatus.BAD_REQUEST,
                    request.getInferenceEntityId()
                )
            );
            return;
        }

        var requestAsMap = requestToMap(request);
        var resolvedTaskType = ServiceUtils.resolveTaskType(request.getTaskType(), (String) requestAsMap.remove(TaskType.NAME));

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

        if (List.of(OLD_ELSER_SERVICE_NAME, ElasticsearchInternalService.NAME).contains(serviceName)) {
            // required for BWC of elser service in elasticsearch service TODO remove when elser service deprecated
            requestAsMap.put(ModelConfigurations.SERVICE, serviceName);
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
                    Messages.INFERENCE_ID_MATCHES_EXISTING_MODEL_IDS_BUT_MUST_NOT,
                    request.getInferenceEntityId()
                )
            );
            return;
        }

        parseAndStoreModel(service.get(), request.getInferenceEntityId(), resolvedTaskType, requestAsMap, request.getTimeout(), listener);
    }

    private void parseAndStoreModel(
        InferenceService service,
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        TimeValue timeout,
        ActionListener<PutInferenceModelAction.Response> listener
    ) {
        ActionListener<Model> storeModelListener = listener.delegateFailureAndWrap(
            (delegate, verifiedModel) -> modelRegistry.storeModel(
                verifiedModel,
                ActionListener.wrap(r -> startInferenceEndpoint(service, timeout, verifiedModel, delegate), e -> {
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
                }),
                timeout
            )
        );

        ActionListener<Model> parsedModelListener = listener.delegateFailureAndWrap((delegate, model) -> {
            if (skipValidationAndStart) {
                storeModelListener.onResponse(model);
            } else {
                ModelValidatorBuilder.buildModelValidator(model.getTaskType(), service instanceof ElasticsearchInternalService)
                    .validate(service, model, timeout, storeModelListener);
            }
        });

        service.parseRequestConfig(inferenceEntityId, taskType, config, parsedModelListener);
    }

    private void startInferenceEndpoint(
        InferenceService service,
        TimeValue timeout,
        Model model,
        ActionListener<PutInferenceModelAction.Response> listener
    ) {
        if (skipValidationAndStart) {
            listener.onResponse(new PutInferenceModelAction.Response(model.getConfigurations()));
        } else {
            service.start(model, timeout, listener.map(started -> new PutInferenceModelAction.Response(model.getConfigurations())));
        }
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

}
