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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;

public class TransportPutCCMConfigurationAction extends TransportMasterNodeAction<
    PutCCMConfigurationAction.Request,
    CCMEnabledActionResponse> {

    private final CCMFeature ccmFeature;
    private final CCMService ccmService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutCCMConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        CCMService ccmService,
        ProjectResolver projectResolver,
        CCMFeature ccmFeature
    ) {
        super(
            PutCCMConfigurationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutCCMConfigurationAction.Request::new,
            CCMEnabledActionResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.ccmService = Objects.requireNonNull(ccmService);
        this.projectResolver = Objects.requireNonNull(projectResolver);
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutCCMConfigurationAction.Request request,
        ClusterState state,
        ActionListener<CCMEnabledActionResponse> listener
    ) {
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            listener.onFailure(CCM_FORBIDDEN_EXCEPTION);
            return;
        }

        var enabledListener = listener.<Void>delegateFailureIgnoreResponseAndWrap(
            delegate -> delegate.onResponse(new CCMEnabledActionResponse(true))
        );

        ccmService.storeConfiguration(new CCMModel(request.getApiKey()), enabledListener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutCCMConfigurationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
