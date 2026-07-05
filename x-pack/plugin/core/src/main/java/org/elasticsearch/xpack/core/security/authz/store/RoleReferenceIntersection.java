/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.OrderedGroupedActionListener;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * This class wraps a list of role references that should be intersected when building the runtime Role.
 */
public class RoleReferenceIntersection {

    private final List<RoleReference> roleReferences;

    public RoleReferenceIntersection(RoleReference... roleReferences) {
        this(List.of(roleReferences));
    }

    public RoleReferenceIntersection(List<RoleReference> roleReferences) {
        assert roleReferences != null && false == roleReferences.isEmpty() : "role references cannot be null or empty";
        this.roleReferences = Objects.requireNonNull(roleReferences);
    }

    public List<RoleReference> getRoleReferences() {
        return roleReferences;
    }

    public void buildRole(BiConsumer<RoleReference, ActionListener<Role>> singleRoleBuilder, ActionListener<Role> roleActionListener) {
        // Role#limitedBy is non-commutative, so the fold must reduce in submission order regardless of
        // the order in which singleRoleBuilder responses arrive.
        OrderedGroupedActionListener.forEach(roleReferences, singleRoleBuilder, roleActionListener.delegateFailureAndWrap((l, roles) -> {
            Role finalRole = roles.getFirst();
            for (int i = 1; i < roles.size(); i++) {
                finalRole = finalRole.limitedBy(roles.get(i));
            }
            l.onResponse(finalRole);
        }));
    }
}
