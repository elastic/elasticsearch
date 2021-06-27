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
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public final class CompositeServiceAccountTokenStore implements ServiceAccountTokenStore {

    private static final Logger logger =
        LogManager.getLogger(CompositeServiceAccountTokenStore.class);

    private final ThreadContext threadContext;
    private final List<ServiceAccountTokenStore> stores;

    public CompositeServiceAccountTokenStore(
        List<ServiceAccountTokenStore> stores, ThreadContext threadContext) {
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
                storeAuthenticationResult -> false == storeAuthenticationResult.isSuccess());
        try {
            authenticatingListener.run();
        } catch (Exception e) {
            logger.debug(new ParameterizedMessage("authentication of service token [{}] failed", token.getQualifiedName()), e);
            listener.onFailure(e);
        }
    }

    @Override
    public void findTokensFor(ServiceAccountId accountId, ActionListener<Collection<TokenInfo>> listener) {
        final CollectingActionListener collector = new CollectingActionListener(accountId, listener);
        try {
            collector.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    class CollectingActionListener implements ActionListener<Collection<TokenInfo>>, Runnable {
        private final ActionListener<Collection<TokenInfo>> delegate;
        private final ServiceAccountId accountId;
        private final List<TokenInfo> result = new ArrayList<>();
        private int position = 0;

        CollectingActionListener(ServiceAccountId accountId, ActionListener<Collection<TokenInfo>> delegate) {
            this.delegate = delegate;
            this.accountId = accountId;
        }

        @Override
        public void run() {
            if (stores.isEmpty()) {
                delegate.onResponse(List.of());
            } else if (position < 0 || position >= stores.size()) {
                onFailure(new IllegalArgumentException("invalid position [" + position + "]. List size [" + stores.size() + "]"));
            } else {
                stores.get(position++).findTokensFor(accountId, this);
            }
        }

        @Override
        public void onResponse(Collection<TokenInfo> response) {
            result.addAll(response);
            if (position == stores.size()) {
                delegate.onResponse(List.copyOf(result));
            } else {
                stores.get(position++).findTokensFor(accountId, this);
            }
        }

        @Override
        public void onFailure(Exception e) {
            delegate.onFailure(e);
        }
    }
}
