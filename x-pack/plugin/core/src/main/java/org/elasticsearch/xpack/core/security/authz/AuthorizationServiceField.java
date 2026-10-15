/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.util.concurrent.ThreadContextTransient;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Collection;
import java.util.List;

public final class AuthorizationServiceField {

    public static final ThreadContextTransient<IndicesAccessControl> INDICES_PERMISSIONS_VALUE = ThreadContextTransient.transientValue(
        "_indices_permissions",
        IndicesAccessControl.class
    );
    public static final ThreadContextTransient<String> ORIGINATING_ACTION_VALUE = ThreadContextTransient.transientValue(
        "_originating_action_name",
        String.class
    );
    public static final ThreadContextTransient<AuthorizationEngine.AuthorizationInfo> AUTHORIZATION_INFO_VALUE = ThreadContextTransient
        .transientValue("_authz_info", AuthorizationEngine.AuthorizationInfo.class);
    /**
     * The profile uid of the effective user, resolved while authorizing an index action whose document level security
     * refers to {@code _user.profile_uid}. Absent when no such query applies or the user has no profile.
     */
    public static final ThreadContextTransient<String> PROFILE_UID_VALUE = ThreadContextTransient.transientValue(
        "_security_profile_uid",
        String.class
    );

    // Most often, transient authorisation headers are scoped (i.e. set, read and cleared) for the authorisation and execution
    // of individual actions (i.e. there is a different scope between the parent and the child actions)
    public static final Collection<String> ACTION_SCOPE_AUTHORIZATION_KEYS = List.of(
        INDICES_PERMISSIONS_VALUE.getKey(),
        AUTHORIZATION_INFO_VALUE.getKey(),
        PROFILE_UID_VALUE.getKey()
    );

    private AuthorizationServiceField() {}
}
