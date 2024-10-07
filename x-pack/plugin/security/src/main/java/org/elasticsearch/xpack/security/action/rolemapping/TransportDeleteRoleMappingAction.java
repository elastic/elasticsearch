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
import org.elasticsearch.common.logging.HeaderWarning;
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
        if (clusterStateRoleMapper.hasMapping(request.getName())) {
            // Since it's allowed to add a mapping with the same name in the native role mapping store as the file_settings namespace,
            // a warning header is added to signal to the caller that this could be a problem.
            HeaderWarning.addWarning(
                "A read only role mapping with the same name ["
                    + request.getName()
                    + "] has been previously been defined in a configuration file. The role mapping ["
                    + request.getName()
                    + "] defined in the configuration file is read only, will not be deleted, and will remain active."
            );
        }
        roleMappingStore.deleteRoleMapping(request, listener.safeMap(DeleteRoleMappingResponse::new));
    }
}
