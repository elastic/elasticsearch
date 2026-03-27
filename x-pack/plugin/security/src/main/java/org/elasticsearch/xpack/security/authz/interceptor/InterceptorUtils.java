/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.RBACEngine;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_VALUE;

class InterceptorUtils {
    public static boolean mayCurrentRoleHaveDlsOrFls(ThreadContext threadContext) {
        final Role role = RBACEngine.maybeGetRBACEngineRole(AUTHORIZATION_INFO_VALUE.get(threadContext));
        // Checking whether role has FLS or DLS first before checking indicesAccessControl for efficiency because indicesAccessControl
        // can contain a long list of indices
        // But if role is null, it means a custom authorization engine is in use and we have to directly go check indicesAccessControl
        return role == null || role.hasFieldOrDocumentLevelSecurity();
    }
}
