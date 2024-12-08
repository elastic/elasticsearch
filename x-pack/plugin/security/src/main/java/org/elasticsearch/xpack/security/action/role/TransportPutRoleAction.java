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
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

public class TransportPutRoleAction extends TransportAction<PutRoleRequest, PutRoleResponse> {

    private final NativeRolesStore rolesStore;

    @Inject
    public TransportPutRoleAction(ActionFilters actionFilters, NativeRolesStore rolesStore, TransportService transportService) {
        super(PutRoleAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(Task task, final PutRoleRequest request, final ActionListener<PutRoleResponse> listener) {
        rolesStore.putRole(request.getRefreshPolicy(), request.roleDescriptor(), listener.safeMap(created -> {
            if (created) {
                logger.info("added role [{}]", request.name());
            } else {
                logger.info("updated role [{}]", request.name());
            }
            return new PutRoleResponse(created);
        }));
    }
}
