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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceServicesAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class TransportGetInferenceServicesAction extends HandledTransportAction<
    GetInferenceServicesAction.Request,
    GetInferenceServicesAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetInferenceServicesAction.class);

    private final InferenceServiceRegistry serviceRegistry;
    private final ElasticInferenceServiceAuthorizationRequestHandler eisAuthorizationRequestHandler;
    private final Sender eisSender;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetInferenceServicesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        InferenceServiceRegistry serviceRegistry,
        ElasticInferenceServiceAuthorizationRequestHandler eisAuthorizationRequestHandler,
        Sender sender
    ) {
        super(
            GetInferenceServicesAction.NAME,
            transportService,
            actionFilters,
            GetInferenceServicesAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceRegistry = serviceRegistry;
        this.eisAuthorizationRequestHandler = eisAuthorizationRequestHandler;
        this.eisSender = sender;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceServicesAction.Request request,
        ActionListener<GetInferenceServicesAction.Response> listener
    ) {
        if (request.getTaskType() == TaskType.ANY) {
            getAllServiceConfigurations(listener);
        } else {
            getServiceConfigurationsForTaskType(request.getTaskType(), listener);
        }
    }

    private void getServiceConfigurationsForTaskType(
        TaskType requestedTaskType,
        ActionListener<GetInferenceServicesAction.Response> listener
    ) {
        var filteredServices = serviceRegistry.getServices()
            .entrySet()
            .stream()
            .filter(
                // Exclude EIS as the EIS specific configurations must be retrieved directly from EIS and merged in later
                service -> service.getValue().name().equals(ElasticInferenceService.NAME) == false
                    && service.getValue().hideFromConfigurationApi() == false
                    && service.getValue().supportedTaskTypes().contains(requestedTaskType)
            )
            .sorted(Comparator.comparing(service -> service.getValue().name()))
            .collect(Collectors.toCollection(ArrayList::new));

        getServiceConfigurationsForServicesAndEis(listener, filteredServices, requestedTaskType);
    }

    private void getAllServiceConfigurations(ActionListener<GetInferenceServicesAction.Response> listener) {
        var availableServices = serviceRegistry.getServices()
            .entrySet()
            .stream()
            .filter(
                // Exclude EIS as the EIS specific configurations must be retrieved directly from EIS and merged in later
                service -> service.getValue().name().equals(ElasticInferenceService.NAME) == false
                    && service.getValue().hideFromConfigurationApi() == false
            )
            .sorted(Comparator.comparing(service -> service.getValue().name()))
            .collect(Collectors.toCollection(ArrayList::new));

        getServiceConfigurationsForServicesAndEis(listener, availableServices, null);
    }

    private void getServiceConfigurationsForServicesAndEis(
        ActionListener<GetInferenceServicesAction.Response> listener,
        ArrayList<Map.Entry<String, InferenceService>> availableServices,
        @Nullable TaskType requestedTaskType
    ) {
        SubscribableListener.<ElasticInferenceServiceAuthorizationModel>newForked(authModelListener -> {
            // Executing on a separate thread because there's a chance the authorization call needs to do some initialization for the Sender
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> getEisAuthorization(authModelListener, eisSender));
        }).<List<InferenceServiceConfiguration>>andThen((configurationListener, authorizationModel) -> {
            var serviceConfigs = getServiceConfigurationsForServices(availableServices);
            serviceConfigs.sort(Comparator.comparing(InferenceServiceConfiguration::getService));

            if (authorizationModel.isAuthorized() == false) {
                configurationListener.onResponse(serviceConfigs);
                return;
            }

            // If there was a requested task type and the authorization response from EIS doesn't support it, we'll exclude EIS as a valid
            // service
            if (requestedTaskType != null && authorizationModel.getTaskTypes().contains(requestedTaskType) == false) {
                configurationListener.onResponse(serviceConfigs);
                return;
            }

            var config = ElasticInferenceService.createConfiguration(authorizationModel.getTaskTypes());
            serviceConfigs.add(config);
            serviceConfigs.sort(Comparator.comparing(InferenceServiceConfiguration::getService));
            configurationListener.onResponse(serviceConfigs);
        })
            .addListener(
                listener.delegateFailureAndWrap(
                    (delegate, configurations) -> delegate.onResponse(new GetInferenceServicesAction.Response(configurations))
                )
            );
    }

    private void getEisAuthorization(ActionListener<ElasticInferenceServiceAuthorizationModel> listener, Sender sender) {
        var disabledServiceListener = listener.delegateResponse((delegate, e) -> {
            logger.warn(
                "Failed to retrieve authorization information from the "
                    + "Elastic Inference Service while determining service configurations. Marking service as disabled.",
                e
            );
            delegate.onResponse(ElasticInferenceServiceAuthorizationModel.unauthorized());
        });

        eisAuthorizationRequestHandler.getAuthorization(disabledServiceListener, sender);
    }

    private List<InferenceServiceConfiguration> getServiceConfigurationsForServices(
        ArrayList<Map.Entry<String, InferenceService>> services
    ) {
        var serviceConfigurations = new ArrayList<InferenceServiceConfiguration>();
        for (var service : services) {
            serviceConfigurations.add(service.getValue().getConfiguration());
        }

        return serviceConfigurations;
    }
}
