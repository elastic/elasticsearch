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
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authc.support.mapper.ProjectStateRoleMapper;

public class TransportDeleteRoleMappingAction extends HandledTransportAction<DeleteRoleMappingRequest, DeleteRoleMappingResponse> {
    private final NativeRoleMappingStore roleMappingStore;
    private final ProjectStateRoleMapper projectStateRoleMapper;

    @Inject
    public TransportDeleteRoleMappingAction(
        ActionFilters actionFilters,
        TransportService transportService,
        NativeRoleMappingStore roleMappingStore,
        ProjectStateRoleMapper projectStateRoleMapper
    ) {
        super(
            DeleteRoleMappingAction.NAME,
            transportService,
            actionFilters,
            DeleteRoleMappingRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.roleMappingStore = roleMappingStore;
        this.projectStateRoleMapper = projectStateRoleMapper;
    }

    @Override
    protected void doExecute(Task task, DeleteRoleMappingRequest request, ActionListener<DeleteRoleMappingResponse> listener) {
        roleMappingStore.deleteRoleMapping(request, listener.safeMap(found -> {
            if (found && projectStateRoleMapper.hasMapping(request.getName())) {
                // Allow to delete a mapping with the same name in the native role mapping store as the file_settings namespace, but
                // add a warning header to signal to the caller that this could be a problem.
                HeaderWarning.addWarning(
                    "A read-only role mapping with the same name ["
                        + request.getName()
                        + "] has previously been defined in a configuration file. "
                        + "The native role mapping was deleted, but the read-only mapping will remain active "
                        + "and will be used to determine role assignments."
                );
            }
            return new DeleteRoleMappingResponse(found);
        }));
    }
}
