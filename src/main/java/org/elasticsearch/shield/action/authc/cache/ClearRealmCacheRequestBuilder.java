/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.authc.cache;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.shield.client.ShieldAuthcClient;
import org.elasticsearch.shield.client.ShieldClient;

/**
 *
 */
public class ClearRealmCacheRequestBuilder extends NodesOperationRequestBuilder<ClearRealmCacheRequest, ClearRealmCacheResponse, ClearRealmCacheRequestBuilder> {

    private final ShieldAuthcClient authcClient;

    public ClearRealmCacheRequestBuilder(ElasticsearchClient client) {
        this(client, ClearRealmCacheAction.INSTANCE);
    }

    public ClearRealmCacheRequestBuilder(ElasticsearchClient client, ClearRealmCacheAction action) {
        super(client, action, new ClearRealmCacheRequest());
        authcClient = new ShieldClient(client).authc();
    }

    /**
     * Sets the realms for which caches will be evicted. When not set all the caches of all realms will be
     * evicted.
     *
     * @param realms    The realm names
     */
    public ClearRealmCacheRequestBuilder realms(String... realms) {
        request.realms(realms);
        return this;
    }

    /**
     * Sets the usernames of the users that should be evicted from the caches. When not set, all users
     * will be evicted.
     *
     * @param usernames The usernames
     */
    public ClearRealmCacheRequestBuilder usernames(String... usernames) {
        request.usernames(usernames);
        return this;
    }
}
