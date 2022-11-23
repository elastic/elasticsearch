/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.privileges;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Determines what privileges a user has within a given {@link ServiceProviderPrivileges service}.
 */
public class UserPrivilegeResolver {

    public static class UserPrivileges {
        public final String principal;
        public final boolean hasAccess;
        public final Set<String> roles;

        public UserPrivileges(String principal, boolean hasAccess, Set<String> roles) {
            this.principal = Objects.requireNonNull(principal, "principal may not be null");
            if (hasAccess == false && roles.isEmpty() == false) {
                throw new IllegalArgumentException("a user without access may not have roles ([" + roles + "])");
            }
            this.hasAccess = hasAccess;
            this.roles = Set.copyOf(Objects.requireNonNull(roles, "roles may not be null"));
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder().append(getClass().getSimpleName())
                .append("{")
                .append(principal)
                .append(", ")
                .append(hasAccess);
            if (hasAccess) {
                str.append(", ").append(roles);
            }
            str.append("}");
            return str.toString();
        }

        public static UserPrivileges noAccess(String principal) {
            return new UserPrivileges(principal, false, Set.of());
        }
    }

    private final Logger logger = LogManager.getLogger(UserPrivilegeResolver.class);
    private final Client client;
    private final SecurityContext securityContext;
    private final ApplicationActionsResolver actionsResolver;

    public UserPrivilegeResolver(Client client, SecurityContext securityContext, ApplicationActionsResolver actionsResolver) {
        this.client = client;
        this.securityContext = securityContext;
        this.actionsResolver = actionsResolver;
    }

    /**
     * Resolves the user's privileges for the specified service.
     * Requires that the active user is set in the {@link org.elasticsearch.xpack.core.security.SecurityContext}.
     */
    public void resolve(ServiceProviderPrivileges service, ActionListener<UserPrivileges> listener) {
        buildResourcePrivilege(service, ActionListener.wrap(resourcePrivilege -> {
            final String username = securityContext.requireUser().principal();
            if (resourcePrivilege == null) {
                listener.onResponse(UserPrivileges.noAccess(username));
                return;
            }
            HasPrivilegesRequest request = new HasPrivilegesRequest();
            request.username(username);
            request.clusterPrivileges(Strings.EMPTY_ARRAY);
            request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
            request.applicationPrivileges(resourcePrivilege);
            client.execute(HasPrivilegesAction.INSTANCE, request, ActionListener.wrap(response -> {
                logger.debug(
                    "Checking access for user [{}] to application [{}] resource [{}]",
                    username,
                    service.getApplicationName(),
                    service.getResource()
                );
                UserPrivileges privileges = buildResult(response, service);
                logger.debug("Resolved service privileges [{}]", privileges);
                listener.onResponse(privileges);
            }, listener::onFailure));
        }, listener::onFailure));

    }

    private UserPrivileges buildResult(HasPrivilegesResponse response, ServiceProviderPrivileges service) {
        final Set<ResourcePrivileges> appPrivileges = response.getApplicationPrivileges().get(service.getApplicationName());
        if (appPrivileges == null || appPrivileges.isEmpty()) {
            return UserPrivileges.noAccess(response.getUsername());
        }

        final Set<String> roles = appPrivileges.stream()
            .filter(rp -> rp.getResource().equals(service.getResource()))
            .map(rp -> rp.getPrivileges().entrySet())
            .flatMap(Set::stream)
            .filter(Map.Entry::getValue)
            .map(Map.Entry::getKey)
            .map(service.getRoleMapping())
            .filter(Objects::nonNull)
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet());
        final boolean hasAccess = roles.isEmpty() == false;
        return new UserPrivileges(response.getUsername(), hasAccess, roles);
    }

    private void buildResourcePrivilege(
        ServiceProviderPrivileges service,
        ActionListener<RoleDescriptor.ApplicationResourcePrivileges> listener
    ) {
        actionsResolver.getActions(service.getApplicationName(), ActionListener.wrap(actions -> {
            if (actions == null || actions.isEmpty()) {
                logger.warn("No application-privilege actions defined for application [{}]", service.getApplicationName());
                listener.onResponse(null);
            } else {
                logger.debug("Using actions [{}] for application [{}]", actions, service.getApplicationName());
                final RoleDescriptor.ApplicationResourcePrivileges.Builder builder = RoleDescriptor.ApplicationResourcePrivileges.builder();
                builder.application(service.getApplicationName());
                builder.resources(service.getResource());
                builder.privileges(actions);
                listener.onResponse(builder.build());
            }
        }, listener::onFailure));
    }
}
