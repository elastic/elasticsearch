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
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.security.authc.support.mapper.ClusterStateRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

public class TransportDeleteRoleMappingAction extends HandledTransportAction<DeleteRoleMappingRequest, DeleteRoleMappingResponse> {

    private final NativeRoleMappingStore roleMappingStore;
    private final ClusterStateRoleMapper clusterStateRoleMapper;

    @Inject
    public TransportDeleteRoleMappingAction(
        ActionFilters actionFilters,
        TransportService transportService,
        NativeRoleMappingStore roleMappingStore,
        ClusterStateRoleMapper clusterStateRoleMapper
    ) {
        super(
            DeleteRoleMappingAction.NAME,
            transportService,
            actionFilters,
            DeleteRoleMappingRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.roleMappingStore = roleMappingStore;
        this.clusterStateRoleMapper = clusterStateRoleMapper;
    }

    @Override
    protected void doExecute(Task task, DeleteRoleMappingRequest request, ActionListener<DeleteRoleMappingResponse> listener) {
        roleMappingStore.deleteRoleMapping(request, listener.delegateFailureAndWrap((l, found) -> {
            // TODO make sure we handle cluster-state blocks appropriately since `getMappings(...)` access cluster-state under the hood
            if (false == found && clusterStateRoleMapper.hasMapping(request.getName())) {
                // TODO perhaps always returning not-found if the role mapping is not found in native store is less confusing
                l.onFailure(
                    new IllegalArgumentException(
                        "You attempted to delete a role mapping ["
                            + request.getName()
                            + "] that exists in the [file_settings] namespace. "
                            + "Deleting role mappings in the [file_settings] namespace is not possible via API."
                    )
                );
                return;
            }
            l.onResponse(new DeleteRoleMappingResponse(found));
        }));
    }
}
