/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.role;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

public class TransportDeleteRoleAction extends HandledTransportAction<DeleteRoleRequest, DeleteRoleResponse> {

    private final NativeRolesStore rolesStore;

    @Inject
    public TransportDeleteRoleAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, NativeRolesStore rolesStore,
                                     TransportService transportService) {
        super(settings, DeleteRoleAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                DeleteRoleRequest::new);
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(DeleteRoleRequest request, ActionListener<DeleteRoleResponse> listener) {
        if (ReservedRolesStore.isReserved(request.name())) {
            listener.onFailure(new IllegalArgumentException("role [" + request.name() + "] is reserved and cannot be deleted"));
            return;
        }

        try {
            rolesStore.deleteRole(request, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean found) {
                    listener.onResponse(new DeleteRoleResponse(found));
                }

                @Override
                public void onFailure(Exception t) {
                    listener.onFailure(t);
                }
            });
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to delete role [{}]", request.name()), e);
            listener.onFailure(e);
        }
    }
}
