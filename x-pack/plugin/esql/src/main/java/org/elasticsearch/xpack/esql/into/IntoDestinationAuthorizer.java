/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Authorizes the caller's write access to the destination of an {@code INTO} before anything about that destination is reported.
 * The checks that run during planning read cluster state, which security does not filter, so without this step a caller who may
 * not write could learn about a destination (whether it exists, its ingest pipeline, its routing) from the errors.
 * <p>
 * The rule mirrors a bulk: the caller needs {@code create_doc} on the destination, and to create a destination that does not exist
 * they also need {@code create_index} or {@code auto_configure}. A caller who may not write gets the denial a bulk would give,
 * the same whether or not the destination exists. Read access to the destination is irrelevant, since {@code INTO} never reads it.
 */
public final class IntoDestinationAuthorizer {
    private static final String WRITE = "create_doc";
    private static final String CREATE_INDEX = "create_index";
    private static final String AUTO_CONFIGURE = "auto_configure";
    // The action a bulk is denied on, so the denial reads like any other denied write
    private static final String BULK_ACTION = "indices:data/write/bulk[s]";

    /** Asks security which of the requested privileges the caller holds. */
    @FunctionalInterface
    public interface PrivilegeChecker {
        void check(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener);
    }

    private final Supplier<String> principal;
    private final PrivilegeChecker checker;
    private final boolean enabled;

    /** Authorizes with the given checker, for the user the supplier names, or {@code null} when there is none. */
    public IntoDestinationAuthorizer(Supplier<String> principal, PrivilegeChecker checker) {
        this(principal, checker, true);
    }

    private IntoDestinationAuthorizer(Supplier<String> principal, PrivilegeChecker checker, boolean enabled) {
        this.principal = principal;
        this.checker = checker;
        this.enabled = enabled;
    }

    /** Passes every destination, for when security is off. */
    public static IntoDestinationAuthorizer disabled() {
        return new IntoDestinationAuthorizer(() -> null, (request, listener) -> listener.onResponse(null), false);
    }

    /** Builds the authorizer that asks the local node's security, or {@link #disabled()} when security is off. */
    public static IntoDestinationAuthorizer create(Settings settings, TransportService transportService, Executor executor) {
        if (settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey()) == false || XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            return disabled();
        }
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        return new IntoDestinationAuthorizer(() -> {
            Authentication authentication = securityContext.getAuthentication();
            return authentication == null ? null : authentication.getEffectiveSubject().getUser().principal();
        },
            (request, listener) -> transportService.sendRequest(
                transportService.getLocalNode(),
                HasPrivilegesAction.NAME,
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, HasPrivilegesResponse::new, executor)
            )
        );
    }

    /**
     * Completes with {@code null} when the caller may write to {@code destination}, and fails with a 403 otherwise.
     *
     * @param destinationExists whether the destination exists now. A missing one has to be created by the write, which needs more.
     */
    public void authorize(String destination, boolean destinationExists, ActionListener<Void> listener) {
        if (enabled == false) {
            listener.onResponse(null);
            return;
        }
        String user = principal.get();
        if (user == null) {
            listener.onFailure(new IllegalStateException("missing or unable to read authentication info on request"));
            return;
        }
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user);
        request.clusterPrivileges();
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        request.indexPrivileges(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(destination)
                .privileges(destinationExists ? new String[] { WRITE } : new String[] { WRITE, CREATE_INDEX, AUTO_CONFIGURE })
                .build()
        );
        checker.check(request, listener.delegateFailureAndWrap((l, response) -> {
            if (isAllowed(response, destination, destinationExists)) {
                l.onResponse(null);
            } else {
                l.onFailure(
                    Exceptions.authorizationError(
                        "action [{}] is unauthorized for user [{}] on indices [{}]",
                        BULK_ACTION,
                        user,
                        destination
                    )
                );
            }
        }));
    }

    private static boolean isAllowed(HasPrivilegesResponse response, String destination, boolean destinationExists) {
        for (ResourcePrivileges resource : response.getIndexPrivileges()) {
            if (resource.getResource().equals(destination) == false) {
                continue;
            }
            if (resource.isAllowed(WRITE) == false) {
                return false;
            }
            // Either privilege lets a write create the destination
            return destinationExists || resource.isAllowed(CREATE_INDEX) || resource.isAllowed(AUTO_CONFIGURE);
        }
        return false;
    }
}
