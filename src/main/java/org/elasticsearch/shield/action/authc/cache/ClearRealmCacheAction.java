/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.authc.cache;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

/**
 *
 */
public class ClearRealmCacheAction extends ClusterAction<ClearRealmCacheRequest, ClearRealmCacheResponse, ClearRealmCacheRequestBuilder> {

    public static final ClearRealmCacheAction INSTANCE = new ClearRealmCacheAction();
    public static final String NAME = "cluster:admin/shield/realm/cache/clear";

    protected ClearRealmCacheAction() {
        super(NAME);
    }

    @Override
    public ClearRealmCacheRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new ClearRealmCacheRequestBuilder(client);
    }

    @Override
    public ClearRealmCacheResponse newResponse() {
        return new ClearRealmCacheResponse();
    }
}
