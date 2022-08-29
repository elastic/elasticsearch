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
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Optional;
import java.util.Set;

public class TransportPutRoleMappingAction extends ReservedStateAwareHandledTransportAction<PutRoleMappingRequest, PutRoleMappingResponse> {

    private final NativeRoleMappingStore roleMappingStore;

    @Inject
    public TransportPutRoleMappingAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ClusterService clusterService,
        NativeRoleMappingStore roleMappingStore
    ) {
        super(PutRoleMappingAction.NAME, clusterService, transportService, actionFilters, PutRoleMappingRequest::new);
        this.roleMappingStore = roleMappingStore;
    }

    @Override
    protected void doExecuteProtected(
        Task task,
        final PutRoleMappingRequest request,
        final ActionListener<PutRoleMappingResponse> listener
    ) {
        roleMappingStore.putRoleMapping(
            request,
            ActionListener.wrap(created -> listener.onResponse(new PutRoleMappingResponse(created)), listener::onFailure)
        );
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedRoleMappingAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(PutRoleMappingRequest request) {
        return Set.of(request.getName());
    }
}
