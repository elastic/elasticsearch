/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.GetCCMConfigurationAction;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;

public class TransportPutCCMConfigurationAction extends TransportMasterNodeAction<
    PutCCMConfigurationAction.Request,
    CCMEnabledActionResponse> {

    private final CCMService ccmService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutCCMConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        CCMService ccmService,
        ProjectResolver projectResolver
    ) {
        super(
            GetCCMConfigurationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutCCMConfigurationAction.Request::new,
            CCMEnabledActionResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ccmService = ccmService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutCCMConfigurationAction.Request request,
        ClusterState state,
        ActionListener<CCMEnabledActionResponse> listener
    ) {
        if (request.getEnabled() != null && request.getEnabled() == false) {
            handleDisablingCCM(listener);
        } else if (request.getApiKey() != null) {
            handleEnablingCCM(request.getApiKey(), listener);
        } else {
            var validationException = new ValidationException();
            listener.onFailure(
                validationException.addValidationError(
                    "Either [enabled] must be false to disable CCM or [api_key] must be provided to enable CCM"
                )
            );
        }
    }

    private void handleDisablingCCM(ActionListener<CCMEnabledActionResponse> listener) {
        var disabledListener = listener.<Void>delegateFailureIgnoreResponseAndWrap(
            delegate -> delegate.onResponse(new CCMEnabledActionResponse(false))
        );

        ccmService.disableCCM(disabledListener);
    }

    private void handleEnablingCCM(SecureString apiKey, ActionListener<CCMEnabledActionResponse> listener) {
        var enabledListener = listener.<Void>delegateFailureIgnoreResponseAndWrap(
            delegate -> delegate.onResponse(new CCMEnabledActionResponse(true))
        );

        ccmService.storeConfiguration(new CCMModel(apiKey), enabledListener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutCCMConfigurationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
