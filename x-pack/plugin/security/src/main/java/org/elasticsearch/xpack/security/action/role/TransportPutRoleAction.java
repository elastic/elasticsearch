/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class TransportPutRoleAction extends TransportAction<PutRoleRequest, PutRoleResponse> {

    private final NativeRolesStore rolesStore;
    private final NamedXContentRegistry xContentRegistry;
    private final ReservedRoleNameChecker reservedRoleNameChecker;

    @Inject
    public TransportPutRoleAction(
        ActionFilters actionFilters,
        NativeRolesStore rolesStore,
        TransportService transportService,
        NamedXContentRegistry xContentRegistry,
        ReservedRoleNameChecker reservedRoleNameChecker
    ) {
        super(PutRoleAction.NAME, actionFilters, transportService.getTaskManager());
        this.rolesStore = rolesStore;
        this.xContentRegistry = xContentRegistry;
        this.reservedRoleNameChecker = reservedRoleNameChecker;
    }

    @Override
    protected void doExecute(Task task, final PutRoleRequest request, final ActionListener<PutRoleResponse> listener) {
        final Exception validationException = validateRequest(request);
        if (validationException != null) {
            listener.onFailure(validationException);
        } else {
            rolesStore.putRole(request, request.roleDescriptor(), listener.safeMap(created -> {
                if (created) {
                    logger.info("added role [{}]", request.name());
                } else {
                    logger.info("updated role [{}]", request.name());
                }
                return new PutRoleResponse(created);
            }));
        }
    }

    private Exception validateRequest(final PutRoleRequest request) {
        // TODO we can remove this -- `execute()` already calls `request.validate()` before `doExecute()`
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            return validationException;
        }
        if (reservedRoleNameChecker.isReserved(request.name())) {
            throw addValidationError("Role [" + request.name() + "] is reserved and may not be used.", null);
        }
        try {
            DLSRoleQueryValidator.validateQueryField(request.roleDescriptor().getIndicesPrivileges(), xContentRegistry);
        } catch (ElasticsearchException | IllegalArgumentException e) {
            return e;
        }
        return null;
    }
}
