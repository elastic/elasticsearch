/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ReservedStateAwareHandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Optional;
import java.util.Set;

public class TransportDeleteRoleMappingAction extends ReservedStateAwareHandledTransportAction<
    DeleteRoleMappingRequest,
    DeleteRoleMappingResponse> {

    private final NativeRoleMappingStore roleMappingStore;

    @Inject
    public TransportDeleteRoleMappingAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ClusterService clusterService,
        NativeRoleMappingStore roleMappingStore
    ) {
        super(DeleteRoleMappingAction.NAME, clusterService, transportService, actionFilters, DeleteRoleMappingRequest::new);
        this.roleMappingStore = roleMappingStore;
    }

    @Override
    protected void doExecuteProtected(Task task, DeleteRoleMappingRequest request, ActionListener<DeleteRoleMappingResponse> listener) {
        roleMappingStore.deleteRoleMapping(
            request,
            listener.delegateFailure((l, found) -> l.onResponse(new DeleteRoleMappingResponse(found)))
        );
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedRoleMappingAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(DeleteRoleMappingRequest request) {
        return Set.of(request.getName());
    }
}
