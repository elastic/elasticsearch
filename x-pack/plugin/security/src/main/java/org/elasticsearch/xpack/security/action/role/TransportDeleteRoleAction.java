/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

public class TransportDeleteRoleAction extends TransportAction<DeleteRoleRequest, DeleteRoleResponse> {

    private final NativeRolesStore rolesStore;
    private final ReservedRoleNameChecker reservedRoleNameChecker;

    @Inject
    public TransportDeleteRoleAction(
        ActionFilters actionFilters,
        NativeRolesStore rolesStore,
        TransportService transportService,
        ReservedRoleNameChecker reservedRoleNameChecker
    ) {
        super(DeleteRoleAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.rolesStore = rolesStore;
        this.reservedRoleNameChecker = reservedRoleNameChecker;
    }

    @Override
    protected void doExecute(Task task, DeleteRoleRequest request, ActionListener<DeleteRoleResponse> listener) {
        if (reservedRoleNameChecker.isReserved(request.name())) {
            listener.onFailure(new IllegalArgumentException("role [" + request.name() + "] is reserved and cannot be deleted"));
            return;
        }

        try {
            rolesStore.deleteRole(request, listener.safeMap(DeleteRoleResponse::new));
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> "failed to delete role [" + request.name() + "]", e);
            listener.onFailure(e);
        }
    }
}
