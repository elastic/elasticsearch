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

class DlsFlsInterceptorUtils {
    public static boolean isCurrentRoleNullOrHasDlsFlsPermissions(ThreadContext threadContext) {
        final Role role = RBACEngine.maybeGetRBACEngineRole(AUTHORIZATION_INFO_VALUE.get(threadContext));
        // if role is null, it means a custom authorization engine is in use, and we have to directly go check indicesAccessControl
        return role == null || role.hasFieldOrDocumentLevelSecurity();
    }
}
