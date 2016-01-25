/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteRoleAction extends HandledTransportAction<DeleteRoleRequest, DeleteRoleResponse> {

    private final ESNativeRolesStore rolesStore;

    @Inject
    public TransportDeleteRoleAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  ESNativeRolesStore rolesStore, TransportService transportService) {
        super(settings, DeleteRoleAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, DeleteRoleRequest::new);
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(DeleteRoleRequest request, ActionListener<DeleteRoleResponse> listener) {
        try {
            rolesStore.removeRole(request, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean found) {
                    listener.onResponse(new DeleteRoleResponse(found));
                }

                @Override
                public void onFailure(Throwable t) {
                    listener.onFailure(t);
                }
            });
        } catch (Exception e) {
            logger.error("failed to delete role [{}]", e);
            listener.onFailure(e);
        }
    }
}
