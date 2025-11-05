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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetCCMConfigurationAction;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;

public class TransportGetCCMConfigurationAction extends HandledTransportAction<
    GetCCMConfigurationAction.Request,
    GetCCMConfigurationAction.Response> {

    private final CCMService ccmService;

    @Inject
    public TransportGetCCMConfigurationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CCMService ccmService
    ) {
        super(
            GetCCMConfigurationAction.NAME,
            transportService,
            actionFilters,
            GetCCMConfigurationAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ccmService = ccmService;
    }

    @Override
    protected void doExecute(Task task, GetCCMConfigurationAction.Request request, ActionListener<GetCCMConfigurationAction.Response> listener) {
        var enabledListener = ActionListener.<Boolean>wrap(
            enabled -> listener.onResponse(new GetCCMConfigurationAction.Response(enabled)),
            listener::onFailure
        );

        ccmService.isEnabled(enabledListener);
    }
}
