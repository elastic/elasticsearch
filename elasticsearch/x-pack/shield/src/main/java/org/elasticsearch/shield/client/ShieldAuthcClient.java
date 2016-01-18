/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheAction;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheRequest;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheRequestBuilder;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheResponse;

/**
 * A client to manage Shield's authentication
 *
 * @deprecated Use {@link ShieldClient} directly instead
 */
@Deprecated
public class ShieldAuthcClient {

    private final ElasticsearchClient client;

    ShieldAuthcClient(ElasticsearchClient client) {
        this.client = client;
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively
     * select the realms (by their unique names) and/or users (by their usernames) that should be evicted.
     */
    public ClearRealmCacheRequestBuilder prepareClearRealmCache() {
        return new ClearRealmCacheRequestBuilder(client);
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively
     * select the realms (by their unique names) and/or users (by their usernames) that should be evicted.
     */
    public void clearRealmCache(ClearRealmCacheRequest request, ActionListener<ClearRealmCacheResponse> listener) {
        client.execute(ClearRealmCacheAction.INSTANCE, request, listener);
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively
     * select the realms (by their unique names) and/or users (by their usernames) that should be evicted.
     */
    public ActionFuture<ClearRealmCacheResponse> clearRealmCache(ClearRealmCacheRequest request) {
        return client.execute(ClearRealmCacheAction.INSTANCE, request);
    }

}
