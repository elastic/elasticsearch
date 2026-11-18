/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.service.AuthorizedServiceAccounts;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.permission.LimitedRole;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageServiceAccountsPrivilege;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Works out which user-managed service accounts a role may read through its
 * {@link ManageServiceAccountsPrivilege} grants, for a read request the role was not granted outright.
 */
final class AuthorizedServiceAccountsResolver {

    private AuthorizedServiceAccountsResolver() {}

    /**
     * Resolves the accounts the role may read, or {@code null} when it may read none, so the request stays denied.
     * <p>
     * A {@link LimitedRole} grants only what both of its roles grant, so each is resolved separately and each that
     * restricts the read contributes its own group of patterns. A role that would grant the request without
     * restriction, through a privilege such as {@code manage_service_account}, contributes no group. The request must
     * not yet be restricted, so that a role's grants do not count as granting it outright.
     */
    @Nullable
    static AuthorizedServiceAccounts resolve(Role role, String action, TransportRequest request, Authentication authentication) {
        final List<Set<String>> patternGroups = new ArrayList<>();
        if (collect(role, action, request, authentication, patternGroups) == false || patternGroups.isEmpty()) {
            return null;
        }
        return new AuthorizedServiceAccounts(patternGroups);
    }

    private static boolean collect(
        Role role,
        String action,
        TransportRequest request,
        Authentication authentication,
        List<Set<String>> patternGroups
    ) {
        if (role instanceof LimitedRole limitedRole) {
            return collect(limitedRole.getBaseRole(), action, request, authentication, patternGroups)
                && collect(limitedRole.getLimitedByRole(), action, request, authentication, patternGroups);
        }
        if (role.checkClusterAction(action, request, authentication)) {
            return true;
        }
        if (role instanceof SimpleRole == false) {
            return false;
        }
        final Set<String> names = role.cluster()
            .privileges()
            .stream()
            .filter(privilege -> privilege instanceof ManageServiceAccountsPrivilege)
            .flatMap(privilege -> ((ManageServiceAccountsPrivilege) privilege).getNames().stream())
            .collect(Collectors.toUnmodifiableSet());
        if (names.isEmpty()) {
            return false;
        }
        patternGroups.add(names);
        return true;
    }
}
