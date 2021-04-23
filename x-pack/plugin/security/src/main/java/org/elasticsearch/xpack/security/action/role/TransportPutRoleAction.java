/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

public class TransportPutRoleAction extends HandledTransportAction<PutRoleRequest, PutRoleResponse> {

    private final NativeRolesStore rolesStore;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportPutRoleAction(ActionFilters actionFilters, NativeRolesStore rolesStore, TransportService transportService,
                                  NamedXContentRegistry xContentRegistry) {
        super(PutRoleAction.NAME, transportService, actionFilters, PutRoleRequest::new);
        this.rolesStore = rolesStore;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, final PutRoleRequest request, final ActionListener<PutRoleResponse> listener) {
        final String name = request.roleDescriptor().getName();
        if (ReservedRolesStore.isReserved(name)) {
            listener.onFailure(new IllegalArgumentException("role [" + name + "] is reserved and cannot be modified."));
            return;
        }

        try {
            DLSRoleQueryValidator.validateQueryField(request.roleDescriptor().getIndicesPrivileges(), xContentRegistry);
        } catch (ElasticsearchException | IllegalArgumentException e) {
            listener.onFailure(e);
            return;
        }

        rolesStore.putRole(request, request.roleDescriptor(), listener.delegateFailure((l, created) -> {
            if (created) {
                logger.info("added role [{}]", request.name());
            } else {
                logger.info("updated role [{}]", request.name());
            }
            l.onResponse(new PutRoleResponse(created));
        }));
    }
}
