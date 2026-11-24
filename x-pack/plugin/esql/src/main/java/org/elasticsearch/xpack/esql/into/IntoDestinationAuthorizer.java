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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.concurrent.Executor;

/**
 * Authorizes the caller's write access to the destination of an {@code INTO} before anything about that destination is reported.
 * The checks that run during planning read cluster state, which security does not filter, and a {@code ROW} source needs no index
 * privilege that would stop the caller first. Without this step a caller who may not write could learn about a destination (whether
 * it exists, its ingest pipeline, its routing) from the errors.
 */
public interface IntoDestinationAuthorizer {

    /**
     * Fails with a 403 if the caller may not write to {@code destination}.
     *
     * @param destination the concrete name the write goes to, with date math already resolved
     */
    void authorize(String destination, boolean destinationExists, ActionListener<Void> listener);

    static IntoDestinationAuthorizer allowAll() {
        return (destination, destinationExists, listener) -> listener.onResponse(null);
    }

    /**
     * Builds the authorizer for this node. Whether security is in effect is decided when a destination is authorized, by whether
     * this node serves the {@code has_privileges} action: only a node with security enabled does, whatever its settings say
     * (the setting defaults to {@code true} even where no security plugin is installed), so there is no setting to get wrong.
     * The decision is delivered on {@code executor}, since the privileges reply arrives on a security thread.
     */
    static IntoDestinationAuthorizer create(TransportService transportService, Executor executor) {
        IntoDestinationAuthorizer secured = new HasPrivilegesIntoDestinationAuthorizer(() -> {
            var securityContext = new SecurityContext(Settings.EMPTY, transportService.getThreadPool().getThreadContext());
            Authentication authentication = securityContext.getAuthentication();
            return authentication == null ? null : authentication.getEffectiveSubject().getUser().principal();
        },
            (request, listener) -> transportService.sendRequest(
                transportService.getLocalNode(),
                HasPrivilegesAction.NAME,
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, HasPrivilegesResponse::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            ),
            executor
        );
        return (destination, destinationExists, listener) -> {
            boolean securityInEffect = transportService.getRequestHandler(HasPrivilegesAction.NAME) != null;
            (securityInEffect ? secured : allowAll()).authorize(destination, destinationExists, listener);
        };
    }
}
