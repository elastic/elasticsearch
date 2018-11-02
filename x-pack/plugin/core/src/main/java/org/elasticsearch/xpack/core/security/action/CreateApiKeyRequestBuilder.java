/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;

/**
 * Request builder for populating a {@link CreateApiKeyRequest}
 */
public final class CreateApiKeyRequestBuilder extends ActionRequestBuilder<CreateApiKeyRequest, CreateApiKeyResponse> {

    public CreateApiKeyRequestBuilder(ElasticsearchClient client) {
        super(client, CreateApiKeyAction.INSTANCE, new CreateApiKeyRequest());
    }

    public CreateApiKeyRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    public CreateApiKeyRequestBuilder setExpiration(TimeValue expiration) {
        request.setExpiration(expiration);
        return this;
    }

    public CreateApiKeyRequestBuilder setRoleDescriptors(List<RoleDescriptor> roleDescriptors) {
        request.setRoleDescriptors(roleDescriptors);
        return this;
    }

    public CreateApiKeyRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        request.setRefreshPolicy(refreshPolicy);
        return this;
    }
}
