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

import java.util.Arrays;

/**
 * Builder for requests to add a role to the administrative index
 */
public class AddRoleRequestBuilder extends ActionRequestBuilder<AddRoleRequest, AddRoleResponse, AddRoleRequestBuilder> {

    public AddRoleRequestBuilder(ElasticsearchClient client) {
        this(client, AddRoleAction.INSTANCE);
    }

    public AddRoleRequestBuilder(ElasticsearchClient client, AddRoleAction action) {
        super(client, action, new AddRoleRequest());
    }

    public AddRoleRequestBuilder name(String name) {
        request.name(name);
        return this;
    }

    public AddRoleRequestBuilder cluster(String... cluster) {
        request.cluster(Arrays.asList(cluster));
        return this;
    }

    public AddRoleRequestBuilder runAs(String... runAsUsers) {
        request.runAs(Arrays.asList(runAsUsers));
        return this;
    }

    public AddRoleRequestBuilder addIndices(String[] indices, String[] privileges, @Nullable String[] fields,
                                            @Nullable BytesReference query) {
        request.addIndex(indices, privileges, fields, query);
        return this;
    }
}
