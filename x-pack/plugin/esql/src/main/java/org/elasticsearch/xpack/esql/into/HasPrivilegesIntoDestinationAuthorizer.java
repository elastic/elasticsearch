/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Authorizes the destination of an {@code INTO} by asking security which index privileges the caller holds on it.
 * <p>
 * The rule mirrors a bulk: the caller needs {@code create_doc} on the destination, and to create a destination that does not exist
 * they also need {@code create_index} or {@code auto_configure}. Read access to the destination is irrelevant, since {@code INTO}
 * never reads it.
 * <p>
 * A caller without {@code create_doc} gets the denial of the item a bulk would index, whether or not the destination exists, so the
 * denial cannot be used to tell the two apart. Only a caller who may write learns more: that creating the destination is denied,
 * which a bulk would report to them too.
 * <p>
 * Limits of asking instead of running a request through the security filter: the denial does not carry the caller's effective roles
 * or the privilege hint of the filter's message, and security audits only the {@code has_privileges} call that was granted, not the
 * denial itself.
 */
public final class HasPrivilegesIntoDestinationAuthorizer implements IntoDestinationAuthorizer {
    private static final String WRITE = "create_doc";
    private static final String CREATE_INDEX = "create_index";
    private static final String AUTO_CONFIGURE = "auto_configure";
    // The action a bulk is denied on for a document it creates, which is how INTO indexes (OpType.CREATE)
    private static final String CREATE_DOCUMENT_ACTION = "indices:data/write/index:op_type/create";
    // The action a bulk is denied on when it has to create the missing index
    private static final String AUTO_CREATE_ACTION = "indices:admin/auto_create";

    public interface PrivilegeChecker {
        void check(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener);
    }

    private final Supplier<String> principal;
    private final PrivilegeChecker checker;
    private final Executor executor;

    public HasPrivilegesIntoDestinationAuthorizer(Supplier<String> principal, PrivilegeChecker checker, Executor executor) {
        this.principal = principal;
        this.checker = checker;
        this.executor = executor;
    }

    @Override
    public void authorize(String destination, boolean destinationExists, ActionListener<Void> listener) {
        try {
            // Security would read a pattern as more than the one index the write targets. This is about the name alone, so it says
            // nothing about any destination.
            MetadataCreateIndexService.validateIndexOrAliasName(destination, InvalidIndexNameException::new);
        } catch (InvalidIndexNameException e) {
            listener.onFailure(e);
            return;
        }
        String user = principal.get();
        if (user == null) {
            listener.onFailure(new IllegalStateException("missing or unable to read authentication info on request"));
            return;
        }
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user);
        request.clusterPrivileges(new String[0]);
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        request.indexPrivileges(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(destination)
                .privileges(destinationExists ? new String[] { WRITE } : new String[] { WRITE, CREATE_INDEX, AUTO_CONFIGURE })
                .build()
        );
        // The reply arrives on whichever thread security answers on, and planning continues from the decision, so it is moved to the
        // thread pool planning expects. The move is forced, as the caller was on that pool already.
        ActionListener<HasPrivilegesResponse> onDecision = new ThreadedActionListener<>(
            executor,
            true,
            listener.delegateFailureAndWrap((l, response) -> {
                String deniedAction = deniedAction(response, destination, destinationExists);
                if (deniedAction == null) {
                    l.onResponse(null);
                } else {
                    String format = "action [{}] is unauthorized for user [{}] on indices [{}]";
                    l.onFailure(Exceptions.authorizationError(format, deniedAction, user, destination));
                }
            })
        );
        checker.check(request, onDecision);
    }

    /** The action to report as denied, or {@code null} when the caller may write. */
    @Nullable
    private static String deniedAction(HasPrivilegesResponse response, String destination, boolean destinationExists) {
        for (ResourcePrivileges resource : response.getIndexPrivileges()) {
            if (resource.getResource().equals(destination) == false) {
                continue;
            }
            if (resource.isAllowed(WRITE) == false) {
                return CREATE_DOCUMENT_ACTION;
            }
            boolean mayCreate = destinationExists || resource.isAllowed(CREATE_INDEX) || resource.isAllowed(AUTO_CONFIGURE);
            return mayCreate ? null : AUTO_CREATE_ACTION;
        }
        return CREATE_DOCUMENT_ACTION;
    }
}
