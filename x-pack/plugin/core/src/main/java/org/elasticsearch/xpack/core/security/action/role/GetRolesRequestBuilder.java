/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Builder for requests to retrieve a role from the security index
 */
public class GetRolesRequestBuilder extends ActionRequestBuilder<GetRolesRequest, GetRolesResponse> {

    public GetRolesRequestBuilder(ElasticsearchClient client) {
        super(client, GetRolesAction.INSTANCE, new GetRolesRequest());
    }

    public GetRolesRequestBuilder names(String... names) {
        request.names(names);
        return this;
    }

    public GetRolesRequestBuilder nativeOnly(boolean nativeOnly) {
        request.nativeOnly(nativeOnly);
        return this;
    }
}
