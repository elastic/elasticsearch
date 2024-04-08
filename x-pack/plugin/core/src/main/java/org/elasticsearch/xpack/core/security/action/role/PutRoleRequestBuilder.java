/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Map;

/**
 * Builder for requests to add a role to the administrative index
 */
public class PutRoleRequestBuilder extends ActionRequestBuilder<PutRoleRequest, PutRoleResponse> {

    public PutRoleRequestBuilder(ElasticsearchClient client) {
        super(client, PutRoleAction.INSTANCE, new PutRoleRequest());
    }

    /**
     * Populate the put role request from the source and the role's name
     */
    public PutRoleRequestBuilder source(String name, BytesReference source, XContentType xContentType) throws IOException {
        // we pass false as last parameter because we want to reject the request if field permissions
        // are given in 2.x syntax
        RoleDescriptor descriptor = RoleDescriptor.parse(name, source, false, xContentType, false);
        assert name.equals(descriptor.getName());
        request.name(name);
        request.cluster(descriptor.getClusterPrivileges());
        request.conditionalCluster(descriptor.getConditionalClusterPrivileges());
        request.addIndex(descriptor.getIndicesPrivileges());
        request.addRemoteIndex(descriptor.getRemoteIndicesPrivileges());
        request.addApplicationPrivileges(descriptor.getApplicationPrivileges());
        request.runAs(descriptor.getRunAs());
        request.metadata(descriptor.getMetadata());
        return this;
    }

    public PutRoleRequestBuilder name(String name) {
        request.name(name);
        return this;
    }

    public PutRoleRequestBuilder cluster(String... cluster) {
        request.cluster(cluster);
        return this;
    }

    public PutRoleRequestBuilder runAs(String... runAsUsers) {
        request.runAs(runAsUsers);
        return this;
    }

    public PutRoleRequestBuilder addIndices(
        String[] indices,
        String[] privileges,
        String[] grantedFields,
        String[] deniedFields,
        @Nullable BytesReference query,
        boolean allowRestrictedIndices
    ) {
        request.addIndex(indices, privileges, grantedFields, deniedFields, query, allowRestrictedIndices);
        return this;
    }

    public PutRoleRequestBuilder metadata(Map<String, Object> metadata) {
        request.metadata(metadata);
        return this;
    }

    public PutRoleRequestBuilder setRefreshPolicy(@Nullable String refreshPolicy) {
        request.setRefreshPolicy(refreshPolicy);
        return this;
    }
}
