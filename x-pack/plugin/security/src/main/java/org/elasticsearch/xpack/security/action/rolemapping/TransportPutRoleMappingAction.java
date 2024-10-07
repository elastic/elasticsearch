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
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.security.authc.support.mapper.ClusterStateRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import static org.elasticsearch.xpack.security.authc.support.mapper.ClusterStateRoleMapper.RESERVED_ROLE_MAPPING_SUFFIX;

public class TransportPutRoleMappingAction extends HandledTransportAction<PutRoleMappingRequest, PutRoleMappingResponse> {

    private final NativeRoleMappingStore roleMappingStore;
    private final ClusterStateRoleMapper clusterStateRoleMapper;

    @Inject
    public TransportPutRoleMappingAction(
        ActionFilters actionFilters,
        TransportService transportService,
        NativeRoleMappingStore roleMappingStore,
        ClusterStateRoleMapper clusterStateRoleMapper
    ) {
        super(PutRoleMappingAction.NAME, transportService, actionFilters, PutRoleMappingRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.roleMappingStore = roleMappingStore;
        this.clusterStateRoleMapper = clusterStateRoleMapper;
    }

    @Override
    protected void doExecute(Task task, final PutRoleMappingRequest request, final ActionListener<PutRoleMappingResponse> listener) {
        validateMappingName(request.getName());
        if (clusterStateRoleMapper.hasMapping(request.getName())) {
            // Allow to define a mapping with the same name in the native role mapping store as the file_settings namespace, but add a
            // warning header to signal to the caller that this could be a problem.
            HeaderWarning.addWarning(
                "A read only role mapping with the same name ["
                    + request.getName()
                    + "] has been previously been defined in a configuration file. "
                    + "Both role mappings will be used to determine role assignments."
            );
        }
        roleMappingStore.putRoleMapping(
            request,
            ActionListener.wrap(created -> listener.onResponse(new PutRoleMappingResponse(created)), listener::onFailure)
        );
    }

    private static void validateMappingName(String mappingName) {
        if (mappingName.endsWith(RESERVED_ROLE_MAPPING_SUFFIX)) {
            throw new IllegalArgumentException(
                "Invalid mapping name [" + mappingName + "]. [" + RESERVED_ROLE_MAPPING_SUFFIX + "] is not an allowed suffix"
            );
        }
    }
}
