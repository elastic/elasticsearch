/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import java.util.List;

/**
 * The interface should be implemented by credential stores of different backends.
 */
public interface ServiceAccountsTokenStore {

    /**
     * Verify the given token for encapsulated service account and credential
     */
    boolean authenticate(ServiceAccountToken token);

    final class CompositeServiceAccountsTokenStore implements ServiceAccountsTokenStore {

        private final List<ServiceAccountsTokenStore> stores;

        public CompositeServiceAccountsTokenStore(List<ServiceAccountsTokenStore> stores) {
            this.stores = stores;
        }

        @Override
        public boolean authenticate(ServiceAccountToken token) {
            return stores.stream().anyMatch(store -> store.authenticate(token));
        }
    }

}
