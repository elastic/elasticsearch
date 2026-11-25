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
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Authorizes the destination of an {@code INTO} by asking security which index privileges the caller holds on it.
 * <p>
 * The rule mirrors a bulk: the caller needs {@code create_doc} on the destination. Read access to the destination is irrelevant,
 * since {@code INTO} never reads it, and so is the privilege to create it, since {@code INTO} does not create indices.
 * <p>
 * A caller without {@code create_doc} gets the denial of the item a bulk would index, whether or not the destination exists, so the
 * denial cannot be used to tell the two apart.
 * <p>
 * Limits of asking instead of running a request through the security filter: the denial does not carry the caller's effective roles
 * or the privilege hint of the filter's message, and security audits only the {@code has_privileges} call that was granted, not the
 * denial itself.
 */
public final class HasPrivilegesIntoDestinationAuthorizer implements IntoDestinationAuthorizer {
    private static final String WRITE = "create_doc";
    // The action a bulk is denied on for a document it creates, which is how INTO indexes (OpType.CREATE)
    private static final String CREATE_DOCUMENT_ACTION = "indices:data/write/index:op_type/create";

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
    public void authorize(String destination, ActionListener<Void> listener) {
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
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder().indices(destination).privileges(WRITE).build());
        // The reply arrives on whichever thread security answers on, and planning continues from the decision, so it is moved to the
        // thread pool planning expects. The move is forced, as the caller was on that pool already.
        ActionListener<HasPrivilegesResponse> onDecision = new ThreadedActionListener<>(
            executor,
            true,
            listener.delegateFailureAndWrap((l, response) -> {
                if (mayWrite(response, destination)) {
                    l.onResponse(null);
                } else {
                    String format = "action [{}] is unauthorized for user [{}] on indices [{}]";
                    l.onFailure(Exceptions.authorizationError(format, CREATE_DOCUMENT_ACTION, user, destination));
                }
            })
        );
        checker.check(request, onDecision);
    }

    private static boolean mayWrite(HasPrivilegesResponse response, String destination) {
        return response.getIndexPrivileges()
            .stream()
            .anyMatch(resource -> resource.getResource().equals(destination) && resource.isAllowed(WRITE));
    }
}
