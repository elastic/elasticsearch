/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransportGetRolesAction extends HandledTransportAction<GetRolesRequest, GetRolesResponse> {

    private final NativeRolesStore nativeRolesStore;
    private final ReservedRolesStore reservedRolesStore;

    @Inject
    public TransportGetRolesAction(ActionFilters actionFilters, NativeRolesStore nativeRolesStore, TransportService transportService,
                                   ReservedRolesStore reservedRolesStore) {
        super(GetRolesAction.NAME, transportService, actionFilters, GetRolesRequest::new);
        this.nativeRolesStore = nativeRolesStore;
        this.reservedRolesStore = reservedRolesStore;
    }

    @Override
    protected void doExecute(Task task, final GetRolesRequest request, final ActionListener<GetRolesResponse> listener) {
        final String[] requestedRoles = request.names();
        final boolean specificRolesRequested = requestedRoles != null && requestedRoles.length > 0;
        final Set<String> rolesToSearchFor = new HashSet<>();
        final List<RoleDescriptor> roles = new ArrayList<>();

        if (specificRolesRequested) {
            for (String role : requestedRoles) {
                if (ReservedRolesStore.isReserved(role)) {
                    RoleDescriptor rd = reservedRolesStore.roleDescriptor(role);
                    if (rd != null) {
                        roles.add(rd);
                    } else {
                        listener.onFailure(new IllegalStateException("unable to obtain reserved role [" + role + "]"));
                        return;
                    }
                } else {
                    rolesToSearchFor.add(role);
                }
            }
        } else {
            roles.addAll(reservedRolesStore.roleDescriptors());
        }

        if (specificRolesRequested && rolesToSearchFor.isEmpty()) {
            // specific roles were requested but they were built in only, no need to hit the store
            listener.onResponse(new GetRolesResponse(roles.toArray(new RoleDescriptor[roles.size()])));
        } else {
            nativeRolesStore.getRoleDescriptors(rolesToSearchFor, ActionListener.wrap((retrievalResult) -> {
                if (retrievalResult.isSuccess()) {
                    roles.addAll(retrievalResult.getDescriptors());
                    listener.onResponse(new GetRolesResponse(roles.toArray(new RoleDescriptor[roles.size()])));
                } else {
                    listener.onFailure(retrievalResult.getFailure());
                }
            }, listener::onFailure));
        }
    }
}
