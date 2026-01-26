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
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.GetCCMConfigurationAction;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;

public class TransportGetCCMConfigurationAction extends HandledTransportAction<
    GetCCMConfigurationAction.Request,
    CCMEnabledActionResponse> {

    private final CCMService ccmService;
    private final CCMFeature ccmFeature;

    @Inject
    public TransportGetCCMConfigurationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CCMService ccmService,
        CCMFeature ccmFeature
    ) {
        super(
            GetCCMConfigurationAction.NAME,
            transportService,
            actionFilters,
            GetCCMConfigurationAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ccmService = Objects.requireNonNull(ccmService);
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
    }

    @Override
    protected void doExecute(Task task, GetCCMConfigurationAction.Request request, ActionListener<CCMEnabledActionResponse> listener) {
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            listener.onFailure(CCM_FORBIDDEN_EXCEPTION);
            return;
        }

        ccmService.isEnabled(
            listener.delegateFailureAndWrap((delegate, enabled) -> delegate.onResponse(new CCMEnabledActionResponse(enabled)))
        );
    }
}
