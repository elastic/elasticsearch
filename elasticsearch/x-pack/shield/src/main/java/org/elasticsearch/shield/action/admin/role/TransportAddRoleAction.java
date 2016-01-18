/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.role;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportAddRoleAction extends HandledTransportAction<AddRoleRequest, AddRoleResponse> {

    private final ESNativeRolesStore rolesStore;

    @Inject
    public TransportAddRoleAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  ESNativeRolesStore rolesStore, TransportService transportService) {
        super(settings, AddRoleAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, AddRoleRequest::new);
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(AddRoleRequest request, ActionListener<AddRoleResponse> listener) {
        rolesStore.addRole(request, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean created) {
                if (created) {
                    logger.info("added role [{}]", request.name());
                } else {
                    logger.info("updated role [{}]", request.name());
                }
                listener.onResponse(new AddRoleResponse(created));
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }
}
