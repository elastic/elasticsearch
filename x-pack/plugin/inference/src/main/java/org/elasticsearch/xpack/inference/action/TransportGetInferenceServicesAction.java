/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceServicesAction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportGetInferenceServicesAction extends HandledTransportAction<
    GetInferenceServicesAction.Request,
    GetInferenceServicesAction.Response> {

    private final InferenceServiceRegistry serviceRegistry;

    @Inject
    public TransportGetInferenceServicesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        InferenceServiceRegistry serviceRegistry
    ) {
        super(
            GetInferenceServicesAction.NAME,
            transportService,
            actionFilters,
            GetInferenceServicesAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceRegistry = serviceRegistry;
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
                service -> service.getValue().hideFromConfigurationApi() == false
                    && service.getValue().supportedTaskTypes().contains(requestedTaskType)
            )
            .sorted(Comparator.comparing(service -> service.getValue().name()))
            .collect(Collectors.toCollection(ArrayList::new));

        getServiceConfigurationsForServices(filteredServices, listener.delegateFailureAndWrap((delegate, configurations) -> {
            delegate.onResponse(new GetInferenceServicesAction.Response(configurations));
        }));
    }

    private void getAllServiceConfigurations(ActionListener<GetInferenceServicesAction.Response> listener) {
        var availableServices = serviceRegistry.getServices()
            .entrySet()
            .stream()
            .filter(service -> service.getValue().hideFromConfigurationApi() == false)
            .sorted(Comparator.comparing(service -> service.getValue().name()))
            .collect(Collectors.toCollection(ArrayList::new));
        getServiceConfigurationsForServices(availableServices, listener.delegateFailureAndWrap((delegate, configurations) -> {
            delegate.onResponse(new GetInferenceServicesAction.Response(configurations));
        }));
    }

    private void getServiceConfigurationsForServices(
        ArrayList<Map.Entry<String, InferenceService>> services,
        ActionListener<List<InferenceServiceConfiguration>> listener
    ) {
        try {
            var serviceConfigurations = new ArrayList<InferenceServiceConfiguration>();
            for (var service : services) {
                serviceConfigurations.add(service.getValue().getConfiguration());
            }
            listener.onResponse(serviceConfigurations.stream().toList());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
