/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import java.util.Objects;
import java.util.Set;

/**
 * A single application-privilege grant on a role, after the role's requested privilege names have been resolved against
 * the stored {@link ApplicationPrivilegeDescriptor}s.
 * <p>
 * The {@link #privilege()} already carries the union automaton of every action the grant authorizes (resolved
 * stored-privilege actions <em>and</em> any raw action patterns written directly under {@code privileges[]}), exposed via
 * {@link ApplicationPrivilege#predicate()}. That automaton is built once during role resolution and cached, so an
 * {@link ImplicitPrivilegesProvider} can test whether a specific action is granted with a single
 * {@code privilege().predicate().test(action)} rather than reconstructing a matcher over the (potentially thousands of)
 * stored actions on the role-build hot path.
 *
 * @param privilege the resolved application privilege (application name, action automaton/predicate)
 * @param resources the resources the grant applies to (e.g. {@code space:default}, or {@code *} for all)
 */
public record ResolvedApplicationPrivilege(ApplicationPrivilege privilege, Set<String> resources) {

    public ResolvedApplicationPrivilege {
        Objects.requireNonNull(privilege, "privilege must not be null");
        Objects.requireNonNull(resources, "resources must not be null");
    }
}
