/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Collection;

/**
 * Provides additional {@link RoleDescriptor.IndicesPrivileges} that should be implicitly
 * granted based on a user's application privileges.
 * <p>
 * During role building, after application privileges are resolved from the privilege store,
 * each registered provider is invoked with each of the user's role descriptors and the stored
 * application privilege definitions. Any returned {@link RoleDescriptor.IndicesPrivileges}
 * entries are merged into the built role as if they had been declared explicitly.
 * <p>
 * Implementations are expected to be side-effect-free and fast; they run on the hot path of
 * role resolution. An exception thrown by a provider propagates out of role building and fails
 * authorization for the affected user.
 */
public interface ImplicitPrivilegesProvider {

    /**
     * Returns additional index privileges that should be implicitly added to the role
     * based on the given role descriptor and its stored application privilege definitions.
     *
     * @param roleDescriptor a single resolved role descriptor
     * @param storedApplicationPrivileges the stored application privilege definitions
     *        referenced by this role descriptor (filtered to only the (application, privilege-name)
     *        pairs declared in the role descriptor's application privileges)
     * @return additional index privileges to merge into the role, or an empty collection if none
     */
    Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    );
}
