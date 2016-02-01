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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportGetRolesAction extends HandledTransportAction<GetRolesRequest, GetRolesResponse> {

    private final ESNativeRolesStore rolesStore;

    @Inject
    public TransportGetRolesAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   ESNativeRolesStore rolesStore, TransportService transportService) {
        super(settings, GetRolesAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, GetRolesRequest::new);
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(GetRolesRequest request, ActionListener<GetRolesResponse> listener) {
        if (request.roles().length == 1) {
            final String rolename = request.roles()[0];
            // We can fetch a single role with a get, much easier
            rolesStore.getRoleDescriptor(rolename, new ActionListener<RoleDescriptor>() {
                @Override
                public void onResponse(RoleDescriptor roleD) {
                    if (roleD == null) {
                        listener.onResponse(new GetRolesResponse());
                    } else {
                        listener.onResponse(new GetRolesResponse(roleD));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("failed to retrieve role [{}]", t, rolename);
                    listener.onFailure(t);
                }
            });
        } else {
            rolesStore.getRoleDescriptors(request.roles(), new ActionListener<List<RoleDescriptor>>() {
                @Override
                public void onResponse(List<RoleDescriptor> roles) {
                    listener.onResponse(new GetRolesResponse(roles));
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("failed to retrieve role [{}]", t,
                            Strings.arrayToDelimitedString(request.roles(), ","));
                }
            });
        }
    }
}
