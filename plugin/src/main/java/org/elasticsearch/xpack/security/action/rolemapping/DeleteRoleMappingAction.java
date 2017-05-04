/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequestBuilder;
import org.elasticsearch.xpack.security.action.role.DeleteRoleResponse;

/**
 * Action for deleting a role-mapping from the
 * {@link org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore}
 */
public class DeleteRoleMappingAction extends Action<DeleteRoleMappingRequest,
        DeleteRoleMappingResponse, DeleteRoleMappingRequestBuilder> {

    public static final DeleteRoleMappingAction INSTANCE = new DeleteRoleMappingAction();
    public static final String NAME = "cluster:admin/xpack/security/role_mapping/delete";

    private DeleteRoleMappingAction() {
        super(NAME);
    }

    @Override
    public DeleteRoleMappingRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new DeleteRoleMappingRequestBuilder(client, this);
    }

    @Override
    public DeleteRoleMappingResponse newResponse() {
        return new DeleteRoleMappingResponse();
    }
}
