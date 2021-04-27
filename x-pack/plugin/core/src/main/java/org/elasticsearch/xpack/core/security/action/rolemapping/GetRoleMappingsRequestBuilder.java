/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for a request to retrieve role-mappings from X-Pack security
 *
 * see org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class GetRoleMappingsRequestBuilder extends ActionRequestBuilder<GetRoleMappingsRequest, GetRoleMappingsResponse> {

    public GetRoleMappingsRequestBuilder(ElasticsearchClient client) {
        super(client, GetRoleMappingsAction.INSTANCE, new GetRoleMappingsRequest());
    }

    public GetRoleMappingsRequestBuilder names(String... names) {
        request.setNames(names);
        return this;
    }
}
