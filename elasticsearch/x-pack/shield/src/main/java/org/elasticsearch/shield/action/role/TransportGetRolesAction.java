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
import org.elasticsearch.shield.authz.permission.KibanaRole;
import org.elasticsearch.shield.authz.store.ReservedRolesStore;
import org.elasticsearch.shield.authz.store.NativeRolesStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class TransportGetRolesAction extends HandledTransportAction<GetRolesRequest, GetRolesResponse> {

    private final NativeRolesStore nativeRolesStore;
    private final ReservedRolesStore reservedRolesStore;

    @Inject
    public TransportGetRolesAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   NativeRolesStore nativeRolesStore, TransportService transportService,
                                   ReservedRolesStore reservedRolesStore) {
        super(settings, GetRolesAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                GetRolesRequest::new);
        this.nativeRolesStore = nativeRolesStore;
        this.reservedRolesStore = reservedRolesStore;
    }

    @Override
    protected void doExecute(final GetRolesRequest request, final ActionListener<GetRolesResponse> listener) {
        final String[] requestedRoles = request.names();
        final boolean specificRolesRequested = requestedRoles != null && requestedRoles.length > 0;
        final List<String> rolesToSearchFor = new ArrayList<>();
        final List<RoleDescriptor> roles = new ArrayList<>();

        if (specificRolesRequested) {
            for (String role : requestedRoles) {
                if (ReservedRolesStore.isReserved(role)) {
                    RoleDescriptor rd = reservedRolesStore.roleDescriptor(role);
                    if (rd != null) {
                        roles.add(rd);
                    } else {
                        // the kibana role name is reseved but is only visible to the Kibana user, so this should be the only null
                        // descriptor. More details in the ReservedRolesStore
                        assert KibanaRole.NAME.equals(role);
                    }
                } else {
                    rolesToSearchFor.add(role);
                }
            }
        } else {
            roles.addAll(reservedRolesStore.roleDescriptors());
        }

        if (rolesToSearchFor.size() == 1) {
            final String rolename = rolesToSearchFor.get(0);
            // We can fetch a single role with a get, much easier
            nativeRolesStore.getRoleDescriptor(rolename, new ActionListener<RoleDescriptor>() {
                @Override
                public void onResponse(RoleDescriptor roleD) {
                    if (roleD != null) {
                        roles.add(roleD);
                    }
                    listener.onResponse(new GetRolesResponse(roles.toArray(new RoleDescriptor[roles.size()])));
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("failed to retrieve role [{}]", t, rolename);
                    listener.onFailure(t);
                }
            });
        } else if (specificRolesRequested && rolesToSearchFor.isEmpty()) {
            // specific roles were requested but they were built in only, no need to hit the store
            listener.onResponse(new GetRolesResponse(roles.toArray(new RoleDescriptor[roles.size()])));
        } else {
            nativeRolesStore.getRoleDescriptors(
                    rolesToSearchFor.toArray(new String[rolesToSearchFor.size()]), new ActionListener<List<RoleDescriptor>>() {
                @Override
                public void onResponse(List<RoleDescriptor> foundRoles) {
                    roles.addAll(foundRoles);
                    listener.onResponse(new GetRolesResponse(roles.toArray(new RoleDescriptor[roles.size()])));
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("failed to retrieve role [{}]", t,
                            Strings.arrayToDelimitedString(request.names(), ","));
                    listener.onFailure(t);
                }
            });
        }
    }
}
