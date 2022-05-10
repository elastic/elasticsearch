/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Map;

/**
 * internal user that manages the security profile index. Has no cluster permission.
 */
public class SecurityProfileUser extends User {

    public static final String NAME = UsernamesField.SECURITY_PROFILE_NAME;
    public static final SecurityProfileUser INSTANCE = new SecurityProfileUser();
    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.SECURITY_PROFILE_ROLE,
        null,
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(".security-profile", "/\\.security-profile-[0-9].*/")
                .privileges("all")
                .allowRestrictedIndices(true)
                .build() },
        null,
        null,
        null,
        MetadataUtils.DEFAULT_RESERVED_METADATA,
        Map.of()
    );

    private SecurityProfileUser() {
        super(NAME, Strings.EMPTY_ARRAY);
        // the following traits, and especially the run-as one, go with all the internal users
        // TODO abstract in a base `InternalUser` class
        assert enabled();
        assert roles() != null && roles().length == 0;
    }

    @Override
    public boolean equals(Object o) {
        return INSTANCE == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public static boolean is(User user) {
        return INSTANCE.equals(user);
    }

}
