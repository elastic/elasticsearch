/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

public class TransportPutRoleMappingAction
        extends HandledTransportAction<PutRoleMappingRequest, PutRoleMappingResponse> {

    private final NativeRoleMappingStore roleMappingStore;

    @Inject
    public TransportPutRoleMappingAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                         TransportService transportService, NativeRoleMappingStore roleMappingStore) {
        super(settings, PutRoleMappingAction.NAME, threadPool, transportService, actionFilters,
            PutRoleMappingRequest::new);
        this.roleMappingStore = roleMappingStore;
    }

    @Override
    protected void doExecute(final PutRoleMappingRequest request,
                             final ActionListener<PutRoleMappingResponse> listener) {
        roleMappingStore.putRoleMapping(request, ActionListener.wrap(
                created -> listener.onResponse(new PutRoleMappingResponse(created)),
                listener::onFailure
        ));
    }
}
