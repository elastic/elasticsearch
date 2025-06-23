/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Objects;
import java.util.Optional;

public class InternalUser extends User {

    private final Optional<RoleDescriptor> localClusterRoleDescriptor;
    private final Optional<RoleDescriptor> remoteAccessRoleDescriptor;

    InternalUser(String username, @Nullable RoleDescriptor localClusterRole) {
        this(username, Optional.ofNullable(localClusterRole), Optional.empty());
    }

    InternalUser(String username, Optional<RoleDescriptor> localClusterRole, Optional<RoleDescriptor> remoteAccessRole) {
        super(username, Strings.EMPTY_ARRAY);
        assert enabled();
        assert roles() != null && roles().length == 0;
        this.localClusterRoleDescriptor = Objects.requireNonNull(localClusterRole);
        this.localClusterRoleDescriptor.ifPresent(rd -> { assert rd.getName().equals(username); });
        this.remoteAccessRoleDescriptor = Objects.requireNonNull(remoteAccessRole);
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    /**
     * The local-cluster role descriptor assigned to this internal user, or {@link Optional#empty()} if this user does not have a role.
     * This {@link RoleDescriptor} defines the privileges that the internal-user has for requests that originate from a node within the
     * local cluster.
     * @see #getRemoteAccessRoleDescriptor()
     */
    public Optional<RoleDescriptor> getLocalClusterRoleDescriptor() {
        return localClusterRoleDescriptor;
    }

    /**
     * The remote-access role descriptor assigned to this internal user, or {@link Optional#empty()} if this user is not permitted to
     * make cross-cluster requests.
     * This {@link RoleDescriptor} defines the privileges that the internal-user has for requests that run on the current cluster, but
     * originate from a node within an external cluster (via CCS/CCR).
     * @see #getLocalClusterRoleDescriptor()
     */
    public Optional<RoleDescriptor> getRemoteAccessRoleDescriptor() {
        return remoteAccessRoleDescriptor;
    }
}
