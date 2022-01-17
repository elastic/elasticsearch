/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.common.IteratingActionListener;

import java.util.List;
import java.util.function.Function;

public final class CompositeServiceAccountTokenStore implements ServiceAccountTokenStore {

    private static final Logger logger = LogManager.getLogger(CompositeServiceAccountTokenStore.class);

    private final ThreadContext threadContext;
    private final List<ServiceAccountTokenStore> stores;

    public CompositeServiceAccountTokenStore(List<ServiceAccountTokenStore> stores, ThreadContext threadContext) {
        this.stores = stores;
        this.threadContext = threadContext;
    }

    @Override
    public void authenticate(ServiceAccountToken token, ActionListener<StoreAuthenticationResult> listener) {
        // TODO: optimize store order based on auth result?
        final IteratingActionListener<StoreAuthenticationResult, ServiceAccountTokenStore> authenticatingListener =
            new IteratingActionListener<>(
                listener,
                (store, successListener) -> store.authenticate(token, successListener),
                stores,
                threadContext,
                Function.identity(),
                storeAuthenticationResult -> false == storeAuthenticationResult.isSuccess()
            );
        try {
            authenticatingListener.run();
        } catch (Exception e) {
            logger.debug(new ParameterizedMessage("authentication of service token [{}] failed", token.getQualifiedName()), e);
            listener.onFailure(e);
        }
    }
}
