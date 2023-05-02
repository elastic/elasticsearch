/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Optional;

public abstract class InternalUser extends User {

    protected InternalUser(String username) {
        super(username, Strings.EMPTY_ARRAY);
        assert enabled();
        assert roles() != null && roles().length == 0;
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
     * The local-cluster role descriptor assigned to this internal user, or {@link Optional#empty()} is this user does not have a role.
     * This {@link RoleDescriptor} defines the privileges that the internal-user has for requests that originate from a node within the
     * local cluster.
     * @see #getRemoteAccessRole()
     */
    public abstract Optional<RoleDescriptor> getLocalClusterRole();

    /**
     * The remote-access role descriptor assigned to this internal user, or {@link Optional#empty()} is this user is not permitted to
     * make cross-cluster requests.
     * This {@link RoleDescriptor} defines the privileges that the internal-user has for requests that run on the current cluster, but
     * originate from a node within an external cluster (via CCS/CCR).
     * @see #getLocalClusterRole()
     */
    public abstract Optional<RoleDescriptor> getRemoteAccessRole();
}
