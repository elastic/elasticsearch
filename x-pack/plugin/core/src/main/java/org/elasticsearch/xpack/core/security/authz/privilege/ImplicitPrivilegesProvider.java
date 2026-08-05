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
 * Each registered provider is invoked with the resolved {@link ResolvedApplicationPrivilege}s to consider, and any
 * returned {@link RoleDescriptor.IndicesPrivileges} are merged in as if they had been declared explicitly. A provider is
 * not invoked when there are no application privileges to consider, so it cannot contribute privileges independent of
 * them.
 * <p>
 * Implementations are expected to be side-effect-free and fast; they run on the hot path of role resolution. An
 * exception thrown by a provider propagates out of role building and fails authorization for the affected user.
 */
public interface ImplicitPrivilegesProvider {

    /**
     * Returns additional index privileges that should be implicitly added to the role based on the
     * given resolved application privileges.
     * <p>
     * Each {@link ResolvedApplicationPrivilege} bundles a resolved {@link ApplicationPrivilege}
     * (whose {@link ApplicationPrivilege#predicate() predicate} already matches every action the
     * grant authorizes — resolved stored-privilege actions as well as raw action patterns written
     * directly under {@code privileges[]}) with the resources it applies to. Providers should test
     * for the action(s) they care about via {@code privilege().predicate().test(action)} rather than
     * rebuilding a matcher, and read the granted resources from {@link ResolvedApplicationPrivilege#resources()}.
     *
     * @param applicationPrivileges the resolved application-privilege grants to consider; never {@code null}
     * @return additional index privileges to merge into the role, or an empty collection if none
     */
    Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    );
}
