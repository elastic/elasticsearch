/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.privilege.SystemPrivilege;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Internal user that is applied to all requests made elasticsearch itself
 */
public class SystemUser extends InternalUser {

    public static final String NAME = UsernamesField.SYSTEM_NAME;

    @Deprecated
    public static final String ROLE_NAME = UsernamesField.SYSTEM_ROLE;

    private static final RoleDescriptor REMOTE_ACCESS_ROLE_DESCRIPTOR = new RoleDescriptor(
        ROLE_NAME + "_cross_cluster_access",
        new String[] { "cross_cluster_search", "cross_cluster_replication" },
        // Needed for CCR background jobs (with system user)
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("*")
                .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
                .allowRestrictedIndices(true)
                .build() },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    /**
     * Package protected to enforce a singleton (private constructor) - use {@link InternalUsers#SYSTEM_USER} instead
     */
    static final SystemUser INSTANCE = new SystemUser();

    private static final Predicate<String> PREDICATE = SystemPrivilege.INSTANCE.predicate();

    private SystemUser() {
        super(NAME, Optional.empty(), Optional.of(REMOTE_ACCESS_ROLE_DESCRIPTOR));
    }

    /**
     * @return {@link Optional#empty()} because the {@code _system} user does not use role based security
     * @see #isAuthorized(String)
     */
    @Override
    public Optional<RoleDescriptor> getLocalClusterRoleDescriptor() {
        return Optional.empty();
    }

    @Deprecated
    public static boolean is(User user) {
        return InternalUsers.SYSTEM_USER.equals(user);
    }

    public static boolean isAuthorized(String action) {
        return PREDICATE.test(action);
    }

    /**
     * The role descriptor intersection in the returned subject info is always empty. Because the privileges of the cross cluster access
     * internal user are static, we set them during role reference resolution instead of needlessly deserializing the role descriptor
     * intersection (see flow starting at {@link Subject#getRoleReferenceIntersection(AnonymousUser)})
     */
    public static CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo(TransportVersion transportVersion, String nodeName) {
        try {
            return new CrossClusterAccessSubjectInfo(
                Authentication.newInternalAuthentication(INSTANCE, transportVersion, nodeName),
                RoleDescriptorsIntersection.EMPTY
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
