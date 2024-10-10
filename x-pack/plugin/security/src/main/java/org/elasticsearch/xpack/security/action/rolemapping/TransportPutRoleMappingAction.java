/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

public class TransportPutRoleMappingAction extends HandledTransportAction<PutRoleMappingRequest, PutRoleMappingResponse> {

    private final NativeRoleMappingStore roleMappingStore;

    @Inject
    public TransportPutRoleMappingAction(
        ActionFilters actionFilters,
        TransportService transportService,
        NativeRoleMappingStore roleMappingStore
    ) {
        super(PutRoleMappingAction.NAME, transportService, actionFilters, PutRoleMappingRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.roleMappingStore = roleMappingStore;
    }

    @Override
    protected void doExecute(Task task, final PutRoleMappingRequest request, final ActionListener<PutRoleMappingResponse> listener) {
        roleMappingStore.putRoleMapping(
            request,
            ActionListener.wrap(created -> listener.onResponse(new PutRoleMappingResponse(created)), listener::onFailure)
        );
    }
}
