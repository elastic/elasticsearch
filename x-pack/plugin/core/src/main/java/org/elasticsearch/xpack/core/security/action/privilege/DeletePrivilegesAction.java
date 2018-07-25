/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for deleting application privileges.
 */
public final class DeletePrivilegesAction
    extends Action<DeletePrivilegesRequest, DeletePrivilegesResponse, DeletePrivilegesRequestBuilder> {

    public static final DeletePrivilegesAction INSTANCE = new DeletePrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/delete";

    private DeletePrivilegesAction() {
        super(NAME);
    }

    @Override
    public DeletePrivilegesResponse newResponse() {
        return new DeletePrivilegesResponse();
    }

    @Override
    public DeletePrivilegesRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new DeletePrivilegesRequestBuilder(client, INSTANCE);
    }
}
