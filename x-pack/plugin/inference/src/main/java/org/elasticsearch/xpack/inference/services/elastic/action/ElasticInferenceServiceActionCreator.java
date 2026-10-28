/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.common.InferencePreferencesCache;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class ElasticInferenceServiceActionCreator {

    private final Sender sender;
    private final ServiceComponents serviceComponents;
    private final CCMAuthenticationApplierFactory ccmAuthenticationApplierFactory;
    private final InferencePreferencesCache inferencePreferencesCache;

    public ElasticInferenceServiceActionCreator(
        Sender sender,
        ServiceComponents serviceComponents,
        CCMAuthenticationApplierFactory ccmAuthenticationApplierFactory,
        InferencePreferencesCache inferencePreferencesCache
    ) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.ccmAuthenticationApplierFactory = Objects.requireNonNull(ccmAuthenticationApplierFactory);
        this.inferencePreferencesCache = Objects.requireNonNull(inferencePreferencesCache);
    }

    public <T extends ElasticInferenceServiceModel> void create(
        T model,
        TraceContext traceContext,
        ActionListener<ExecutableAction> listener
    ) {
        ccmAuthenticationApplierFactory.getAuthenticationApplier(
            listener.delegateFailureAndWrap(
                (applierListener, applier) -> inferencePreferencesCache.get(
                    applierListener.delegateFailureAndWrap((preferencesListener, preferences) -> {
                        var strategy = ModelStrategyFactory.getStrategy(model);
                        var requestManager = strategy.createRequestManager(model, serviceComponents, traceContext, preferences, applier);
                        preferencesListener.onResponse(
                            new SenderExecutableAction(
                                sender,
                                requestManager,
                                constructFailedToSendRequestMessage(strategy.requestDescription())
                            )
                        );
                    })
                )
            )
        );
    }
}
