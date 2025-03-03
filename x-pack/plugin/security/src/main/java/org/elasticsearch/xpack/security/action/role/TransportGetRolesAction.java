/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetRolesAction extends TransportAction<GetRolesRequest, GetRolesResponse> {

    private final NativeRolesStore nativeRolesStore;
    private final ReservedRoleNameChecker reservedRoleNameChecker;

    @Inject
    public TransportGetRolesAction(
        ActionFilters actionFilters,
        NativeRolesStore nativeRolesStore,
        ReservedRoleNameChecker reservedRoleNameChecker,
        TransportService transportService
    ) {
        super(GetRolesAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nativeRolesStore = nativeRolesStore;
        this.reservedRoleNameChecker = reservedRoleNameChecker;
    }

    @Override
    protected void doExecute(Task task, final GetRolesRequest request, final ActionListener<GetRolesResponse> listener) {
        final String[] requestedRoles = request.names();
        final boolean specificRolesRequested = requestedRoles != null && requestedRoles.length > 0;

        if (request.nativeOnly()) {
            final Set<String> rolesToSearchFor = specificRolesRequested
                ? Arrays.stream(requestedRoles).filter(r -> false == reservedRoleNameChecker.isReserved(r)).collect(Collectors.toSet())
                : Collections.emptySet();
            if (specificRolesRequested && rolesToSearchFor.isEmpty()) {
                // specific roles were requested, but they were all reserved, no need to hit the native store
                listener.onResponse(new GetRolesResponse());
            } else {
                getNativeRoles(rolesToSearchFor, listener);
            }
            return;
        }

        final Set<String> rolesToSearchFor = new LinkedHashSet<>();
        final Set<RoleDescriptor> reservedRoles = new LinkedHashSet<>();
        if (specificRolesRequested) {
            for (String role : requestedRoles) {
                if (reservedRoleNameChecker.isReserved(role)) {
                    RoleDescriptor rd = ReservedRolesStore.roleDescriptor(role);
                    if (rd != null) {
                        reservedRoles.add(rd);
                    }
                } else {
                    rolesToSearchFor.add(role);
                }
            }
        } else {
            reservedRoles.addAll(ReservedRolesStore.roleDescriptors());
        }

        if (specificRolesRequested && rolesToSearchFor.isEmpty()) {
            // specific roles were requested, but they were built in only, no need to hit the store
            listener.onResponse(new GetRolesResponse(reservedRoles.toArray(new RoleDescriptor[0])));
        } else {
            getNativeRoles(rolesToSearchFor, reservedRoles, listener);
        }
    }

    private void getNativeRoles(Set<String> rolesToSearchFor, ActionListener<GetRolesResponse> listener) {
        getNativeRoles(rolesToSearchFor, new LinkedHashSet<>(), listener);
    }

    private void getNativeRoles(Set<String> rolesToSearchFor, Set<RoleDescriptor> foundRoles, ActionListener<GetRolesResponse> listener) {
        nativeRolesStore.getRoleDescriptors(rolesToSearchFor, ActionListener.wrap((retrievalResult) -> {
            if (retrievalResult.isSuccess()) {
                foundRoles.addAll(retrievalResult.getDescriptors());
                listener.onResponse(new GetRolesResponse(foundRoles.toArray(new RoleDescriptor[0])));
            } else {
                listener.onFailure(retrievalResult.getFailure());
            }
        }, listener::onFailure));
    }
}
