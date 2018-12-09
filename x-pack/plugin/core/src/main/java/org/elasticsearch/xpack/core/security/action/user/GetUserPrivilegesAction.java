/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action that lists the set of privileges held by a user.
 */
public final class GetUserPrivilegesAction
    extends Action<GetUserPrivilegesRequest, GetUserPrivilegesResponse, GetUserPrivilegesRequestBuilder> {

    public static final GetUserPrivilegesAction INSTANCE = new GetUserPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/user/list_privileges";

    private GetUserPrivilegesAction() {
        super(NAME);
    }

    @Override
    public GetUserPrivilegesResponse newResponse() {
        return new GetUserPrivilegesResponse();
    }

    @Override
    public GetUserPrivilegesRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetUserPrivilegesRequestBuilder(client);
    }
}
