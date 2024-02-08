/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.operator.RoleDescriptorValidatorFactory;

public class AlternativeTransportPutRoleAction extends HandledTransportAction<PutRoleRequest, PutRoleResponse> {
    private final NativeRolesStore rolesStore;
    private final RoleDescriptorValidatorFactory.RoleDescriptorValidator roleDescriptorValidator;

    @Inject
    public AlternativeTransportPutRoleAction(
        ActionFilters actionFilters,
        NativeRolesStore rolesStore,
        TransportService transportService,
        RoleDescriptorValidatorFactory.RoleDescriptorValidator roleDescriptorValidator
    ) {
        super(PutRoleAction.NAME, transportService, actionFilters, PutRoleRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.rolesStore = rolesStore;
        this.roleDescriptorValidator = roleDescriptorValidator;
    }

    public AlternativeTransportPutRoleAction(
        ActionFilters actionFilters,
        NativeRolesStore rolesStore,
        TransportService transportService,
        NamedXContentRegistry xContentRegistry
    ) {
        this(actionFilters, rolesStore, transportService, new RoleDescriptorValidatorFactory.RoleDescriptorValidator(xContentRegistry));
    }

    @Override
    protected void doExecute(Task task, final PutRoleRequest request, final ActionListener<PutRoleResponse> listener) {
        final Exception validationException = roleDescriptorValidator.validate(request.roleDescriptor());
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

}
