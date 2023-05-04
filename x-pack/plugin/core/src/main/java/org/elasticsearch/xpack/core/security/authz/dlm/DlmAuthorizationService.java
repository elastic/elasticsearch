/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.dlm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.dlm.AuthorizeDlmConfigurationRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;

public class DlmAuthorizationService {
    private final ClusterService clusterService;
    private final SecurityContext securityContext;
    private final Client client;

    public DlmAuthorizationService(ClusterService clusterService, SecurityContext securityContext, Client client) {
        this.clusterService = clusterService;
        this.securityContext = securityContext;
        this.client = client;
    }

    public void authorize(AuthorizeDlmConfigurationRequest request, ActionListener<Void> listener) {
        String[] dataStreamPatterns = request.dataStreamPatterns(clusterService.state());
        // TODO hack hack hack
        if (dataStreamPatterns == null) {
            listener.onResponse(null);
            return;
        }

        RoleDescriptor.IndicesPrivileges[] indicesPrivilegesToCheck = dataStreamPatterns.length == 0
            ? new RoleDescriptor.IndicesPrivileges[0]
            : new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices(dataStreamPatterns).privileges("manage").build() };
        HasPrivilegesRequest hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(securityContext.getAuthentication().getEffectiveSubject().getUser().principal());
        hasPrivilegesRequest.clusterPrivileges("manage_dlm");
        hasPrivilegesRequest.indexPrivileges(indicesPrivilegesToCheck);
        hasPrivilegesRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);

        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, ActionListener.wrap(hasPrivilegesResponse -> {
            if (hasPrivilegesResponse.isCompleteMatch()) {
                listener.onResponse(null);
            } else {
                // TODO detailed failure message here
                listener.onFailure(Exceptions.authorizationError("insufficient privileges to configure DLM"));
            }
        }, listener::onFailure));
    }
}
