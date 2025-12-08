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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.ValidationAuthenticationFactory;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.action.CCMActionUtils.isClusterUpgradedToSupportEnablementService;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION;

public class TransportPutCCMConfigurationAction extends TransportMasterNodeAction<
    PutCCMConfigurationAction.Request,
    CCMEnabledActionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutCCMConfigurationAction.class);
    static final String FAILED_VALIDATION_MESSAGE = "Failed to validate the Cloud Connected Mode API key";

    private final CCMFeature ccmFeature;
    private final CCMService ccmService;
    private final ProjectResolver projectResolver;
    private final Sender eisSender;
    private final ElasticInferenceServiceSettings eisSettings;
    private final FeatureService featureService;

    @Inject
    public TransportPutCCMConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        CCMService ccmService,
        ProjectResolver projectResolver,
        CCMFeature ccmFeature,
        Sender eisSender,
        ElasticInferenceServiceSettings eisSettings,
        FeatureService featureService
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
        this.eisSender = Objects.requireNonNull(eisSender);
        this.eisSettings = Objects.requireNonNull(eisSettings);
        this.featureService = Objects.requireNonNull(featureService);
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

        if (isClusterUpgradedToSupportEnablementService(state, featureService) == false) {
            listener.onFailure(CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION);
            return;
        }

        SubscribableListener.<ElasticInferenceServiceAuthorizationModel>newForked(authValidationListener -> {
            var authRequestHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
                eisSettings.getElasticInferenceServiceUrl(),
                threadPool,
                new ValidationAuthenticationFactory(request.getApiKey())
            );

            var errorListener = authValidationListener.delegateResponse((delegate, exception) -> {
                // The exception will likely be a RetryException, so unwrap it to get to the real cause
                var unwrappedException = ExceptionsHelper.unwrapCause(exception);

                logger.atWarn().withThrowable(unwrappedException).log(FAILED_VALIDATION_MESSAGE);

                if (unwrappedException instanceof ElasticsearchStatusException statusException) {
                    delegate.onFailure(
                        new ElasticsearchStatusException(FAILED_VALIDATION_MESSAGE, statusException.status(), statusException)
                    );
                    return;
                }

                delegate.onFailure(new ElasticsearchStatusException(FAILED_VALIDATION_MESSAGE, RestStatus.BAD_REQUEST, unwrappedException));
            });

            authRequestHandler.getAuthorization(errorListener, eisSender);
        }).<CCMEnabledActionResponse>andThen((storeConfigurationListener) -> {
            var enabledListener = storeConfigurationListener.<Void>delegateFailureIgnoreResponseAndWrap(
                delegate -> delegate.onResponse(new CCMEnabledActionResponse(true))
            );

            ccmService.storeConfiguration(new CCMModel(request.getApiKey()), enabledListener);
        }).addListener(listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutCCMConfigurationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
