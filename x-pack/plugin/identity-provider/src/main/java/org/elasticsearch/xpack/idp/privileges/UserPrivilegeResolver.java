/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.privileges;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Determines what privileges a user has within a given {@link ServiceProviderPrivileges service}.
 */
public class UserPrivilegeResolver {

    public static class UserPrivileges {
        public final String principal;
        public final boolean hasAccess;
        public final Set<String> groups;

        public UserPrivileges(String principal, boolean hasAccess, Set<String> groups) {
            this.principal = Objects.requireNonNull(principal, "principal may not be null");
            if (hasAccess == false && groups.isEmpty() == false) {
                throw new IllegalArgumentException("a user without access ([" + hasAccess + "]) may not have groups ([" + groups + "])");
            }
            this.hasAccess = hasAccess;
            this.groups = Set.copyOf(Objects.requireNonNull(groups, "groups may not be null"));
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder()
                .append(getClass().getSimpleName())
                .append("{")
                .append(principal)
                .append(", ")
                .append(hasAccess);
            if (hasAccess) {
                str.append(", ").append(groups);
            }
            str.append("}");
            return str.toString();
        }

        public static UserPrivileges noAccess(String principal) {
            return new UserPrivileges(principal, false, Set.of());
        }
    }

    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private final SecurityContext securityContext;

    public UserPrivilegeResolver(Client client, SecurityContext securityContext) {
        this.client = client;
        this.securityContext = securityContext;
    }

    /**
     * Resolves the user's privileges for the specified service.
     * Requires that the active user is set in the {@link org.elasticsearch.xpack.core.security.SecurityContext}.
     */
    public void resolve(ServiceProviderPrivileges service, ActionListener<UserPrivileges> listener) {
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        final String username = securityContext.requireUser().principal();
        request.username(username);
        request.applicationPrivileges(buildResourcePrivilege(service));
        client.execute(HasPrivilegesAction.INSTANCE, request, ActionListener.wrap(
            response -> {
                logger.debug("Checking access for user [{}] to application [{}] resource [{}]",
                    username, service.getApplicationName(), service.getResource());
                UserPrivileges privileges = buildResult(response, service);
                logger.debug("Resolved service privileges [{}]", privileges);
                listener.onResponse(privileges);
            },
            listener::onFailure
        ));
    }

    private UserPrivileges buildResult(HasPrivilegesResponse response, ServiceProviderPrivileges service) {
        final Set<ResourcePrivileges> appPrivileges = response.getApplicationPrivileges().get(service.getApplicationName());
        if (appPrivileges == null || appPrivileges.isEmpty()) {
            return UserPrivileges.noAccess(response.getUsername());
        }
        final boolean hasAccess = checkAccess(appPrivileges, service.getLoginAction(), service.getResource());
        if (hasAccess == false) {
            return UserPrivileges.noAccess(response.getUsername());
        }
        final Set<String> groups = service.getGroupActions().entrySet().stream()
            .filter(entry -> checkAccess(appPrivileges, entry.getValue(), service.getResource()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
        return new UserPrivileges(response.getUsername(), hasAccess, groups);
    }

    private boolean checkAccess(Set<ResourcePrivileges> userPrivileges, String action, String resource) {
        final Optional<ResourcePrivileges> match = userPrivileges.stream()
            .filter(rp -> rp.getResource().equals(resource))
            .filter(rp -> rp.isAllowed(action))
            .findAny();
        match.ifPresent(rp -> logger.debug("User has access to [{} on {}] via [{}]", action, resource, rp));
        return match.isPresent();
    }

    private RoleDescriptor.ApplicationResourcePrivileges buildResourcePrivilege(ServiceProviderPrivileges service) {
        final RoleDescriptor.ApplicationResourcePrivileges.Builder builder = RoleDescriptor.ApplicationResourcePrivileges.builder();
        builder.application(service.getApplicationName());
        builder.resources(service.getResource());
        Set<String> actions = new HashSet<>(1 + service.getGroupActions().size());
        actions.add(service.getLoginAction());
        actions.addAll(service.getGroupActions().values());
        builder.privileges(actions);
        return builder.build();
    }
}
