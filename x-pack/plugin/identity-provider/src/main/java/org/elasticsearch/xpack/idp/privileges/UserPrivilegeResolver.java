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
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;

import java.util.Arrays;
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

    private static final Logger logger = LogManager.getLogger(UserPrivilegeResolver.class);
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
        buildResourcePrivilege(service, listener.delegateFailureAndWrap((delegate, resourcePrivilege) -> {
            final String username = securityContext.requireUser().principal();
            if (resourcePrivilege == null) {
                delegate.onResponse(UserPrivileges.noAccess(username));
                return;
            }
            HasPrivilegesRequest request = new HasPrivilegesRequest();
            request.username(username);
            request.clusterPrivileges(Strings.EMPTY_ARRAY);
            request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
            request.applicationPrivileges(resourcePrivilege);
            client.execute(HasPrivilegesAction.INSTANCE, request, delegate.delegateFailureAndWrap((l, response) -> {
                logger.debug(
                    "Checking access for user [{}] to application [{}] resource [{}]",
                    username,
                    service.getApplicationName(),
                    service.getResource()
                );
                UserPrivileges privileges = buildResult(response, service);
                logger.debug("Resolved service privileges [{}]", privileges);
                l.onResponse(privileges);
            }));
        }));

    }

    private static UserPrivileges buildResult(HasPrivilegesResponse response, ServiceProviderPrivileges service) {
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
        var groupedListener = new GroupedActionListener<Set<String>>(2, listener.delegateFailureAndWrap((delegate, actionSets) -> {
            final Set<String> actions = actionSets.stream().flatMap(Set::stream).collect(Collectors.toUnmodifiableSet());
            if (actions == null || actions.isEmpty()) {
                logger.warn("No application-privilege actions defined for application [{}]", service.getApplicationName());
                delegate.onResponse(null);
            } else {
                logger.debug("Using actions [{}] for application [{}]", actions, service.getApplicationName());
                final RoleDescriptor.ApplicationResourcePrivileges.Builder builder = RoleDescriptor.ApplicationResourcePrivileges.builder();
                builder.application(service.getApplicationName());
                builder.resources(service.getResource());
                builder.privileges(actions);
                delegate.onResponse(builder.build());
            }
        }));

        // We need to enumerate possible actions that might be authorized for the user. Here we combine actions that
        // have been granted to the user via roles and other actions that are registered privileges for the given
        // application. These actions will be checked by a has-privileges check above
        final GetUserPrivilegesRequest request = new GetUserPrivilegesRequestBuilder(client).username(securityContext.getUser().principal())
            .request();
        client.execute(
            GetUserPrivilegesAction.INSTANCE,
            request,
            groupedListener.map(
                userPrivileges -> userPrivileges.getApplicationPrivileges()
                    .stream()
                    .filter(appPriv -> appPriv.getApplication().equals(service.getApplicationName()))
                    .map(appPriv -> appPriv.getPrivileges())
                    .flatMap(Arrays::stream)
                    .collect(Collectors.toUnmodifiableSet())
            )
        );
        actionsResolver.getActions(service.getApplicationName(), groupedListener);
    }
}
