/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.realm;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class ClearRealmCacheRequestBuilder extends NodesOperationRequestBuilder<
    ClearRealmCacheRequest,
    ClearRealmCacheResponse,
    ClearRealmCacheRequestBuilder> {

    public ClearRealmCacheRequestBuilder(ElasticsearchClient client) {
        super(client, ClearRealmCacheAction.INSTANCE, new ClearRealmCacheRequest());

    }

    /**
     * Sets the realms for which caches will be evicted. When not set all the caches of all realms will be
     * evicted.
     *
     * @param realms The realm names
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
