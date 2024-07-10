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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.role.BulkDeleteRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

public class TransportBulkDeleteRolesAction extends TransportAction<BulkDeleteRolesRequest, BulkRolesResponse> {

    private final NativeRolesStore rolesStore;

    @Inject
    public TransportBulkDeleteRolesAction(ActionFilters actionFilters, NativeRolesStore rolesStore, TransportService transportService) {
        super(ActionTypes.BULK_DELETE_ROLES.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(Task task, BulkDeleteRolesRequest request, ActionListener<BulkRolesResponse> listener) {
        rolesStore.deleteRoles(request.getRoleNames(), request.getRefreshPolicy(), listener);
    }
}
