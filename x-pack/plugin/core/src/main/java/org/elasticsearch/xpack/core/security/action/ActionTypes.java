/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.action.user.QueryUserResponse;

/**
 * A collection of actions types for the Security plugin that need to be available in xpack.core.security and thus cannot be stored
 * directly with their transport action implementation.
 */
public final class ActionTypes {
    private ActionTypes() {};

    public static final ActionType<ActionResponse.Empty> RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION = new ActionType<>(
        "cluster:admin/xpack/security/remote_cluster_credentials/reload"
    );

    public static final ActionType<QueryUserResponse> QUERY_USER_ACTION = new ActionType<>("cluster:admin/xpack/security/user/query");

    public static final ActionType<BulkRolesResponse> BULK_PUT_ROLES = new ActionType<>("cluster:admin/xpack/security/role/bulk_put");
    public static final ActionType<BulkRolesResponse> BULK_DELETE_ROLES = new ActionType<>("cluster:admin/xpack/security/role/bulk_delete");
}
