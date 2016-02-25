/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * Builder for requests to add a role to the administrative index
 */
public class PutRoleRequestBuilder extends ActionRequestBuilder<PutRoleRequest, PutRoleResponse, PutRoleRequestBuilder> {

    public PutRoleRequestBuilder(ElasticsearchClient client) {
        this(client, PutRoleAction.INSTANCE);
    }

    public PutRoleRequestBuilder(ElasticsearchClient client, PutRoleAction action) {
        super(client, action, new PutRoleRequest());
    }

    public PutRoleRequestBuilder source(String name, BytesReference source) throws Exception {
        request.source(name, source);
        return this;
    }

    public PutRoleRequestBuilder name(String name) {
        request.name(name);
        return this;
    }

    public PutRoleRequestBuilder cluster(String... clusterPrivileges) {
        request.cluster(clusterPrivileges);
        return this;
    }

    public PutRoleRequestBuilder runAs(String... runAsUsers) {
        request.runAs(runAsUsers);
        return this;
    }

    public PutRoleRequestBuilder addIndices(String[] indices, String[] privileges, @Nullable String[] fields,
                                            @Nullable BytesReference query) {
        request.addIndex(indices, privileges, fields, query);
        return this;
    }
}
