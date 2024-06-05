/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

import java.util.Collection;
import java.util.List;

public final class AuthorizationServiceField {

    public static final String INDICES_PERMISSIONS_KEY = "_indices_permissions";
    public static final String ORIGINATING_ACTION_KEY = "_originating_action_name";
    public static final String AUTHORIZATION_INFO_KEY = "_authz_info";

    // Most often, transient authorisation headers are scoped (i.e. set, read and cleared) for the authorisation and execution
    // of individual actions (i.e. there is a different scope between the parent and the child actions)
    public static final Collection<String> ACTION_SCOPE_AUTHORIZATION_KEYS = List.of(INDICES_PERMISSIONS_KEY, AUTHORIZATION_INFO_KEY);

    private AuthorizationServiceField() {}
}
