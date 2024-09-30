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
import org.elasticsearch.xpack.core.security.action.role.QueryRoleResponse;
import org.elasticsearch.xpack.core.security.action.user.QueryUserResponse;

/**
 * A collection of actions types for the Security plugin that need to be available in xpack.core.security and thus cannot be stored
 * directly with their transport action implementation.
 */
public final class ActionTypes {
    private ActionTypes() {};

    // Note: this action is *not* prefixed with `cluster:admin/xpack/security` since it would otherwise be excluded from the `manage`
    // privilege -- instead it matches its prefix to `TransportNodesReloadSecureSettingsAction` which is the "parent" transport action
    // that invokes the overall reload flow.
    // This allows us to maintain the invariant that the parent reload secure settings action can be executed with the `manage` privilege
    // without trappy system-context switches.
    public static final ActionType<ActionResponse.Empty> RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION = new ActionType<>(
        "cluster:admin/nodes/reload_secure_settings/security/remote_cluster_credentials"
    );

    public static final ActionType<QueryUserResponse> QUERY_USER_ACTION = new ActionType<>("cluster:admin/xpack/security/user/query");
    public static final ActionType<BulkRolesResponse> BULK_PUT_ROLES = new ActionType<>("cluster:admin/xpack/security/role/bulk_put");
    public static final ActionType<QueryRoleResponse> QUERY_ROLE_ACTION = new ActionType<>("cluster:admin/xpack/security/role/query");
    public static final ActionType<BulkRolesResponse> BULK_DELETE_ROLES = new ActionType<>("cluster:admin/xpack/security/role/bulk_delete");
}
