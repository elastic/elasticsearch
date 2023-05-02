/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Map;
import java.util.Optional;

/**
 * internal user that manages the security profile index. Has no cluster permission.
 */
public class SecurityProfileUser extends InternalUser {

    public static final SecurityProfileUser INSTANCE = new SecurityProfileUser();

    public static final String NAME = UsernamesField.SECURITY_PROFILE_NAME;
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
        super(NAME);
    }

    @Override
    public Optional<RoleDescriptor> getLocalClusterRole() {
        return Optional.of(ROLE_DESCRIPTOR);
    }

    /**
     * @return {@link Optional#empty()} because this user is not permitted to execute actions that originate from a remote cluster
     */
    @Override
    public Optional<RoleDescriptor> getRemoteAccessRole() {
        return Optional.empty();
    }

}
