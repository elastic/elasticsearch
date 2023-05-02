/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.dlm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.dlm.AuthorizeDataLifecycleAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Exceptions;

public class TransportAuthorizeDataLifecycleAction extends HandledTransportAction<
    AuthorizeDataLifecycleAction.Request,
    AcknowledgedResponse> {

    private final SecurityContext securityContext;
    private final Client client;

    @Inject
    public TransportAuthorizeDataLifecycleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SecurityContext securityContext,
        Client client
    ) {
        super(AuthorizeDataLifecycleAction.NAME, transportService, actionFilters, AuthorizeDataLifecycleAction.Request::new);
        this.securityContext = securityContext;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, AuthorizeDataLifecycleAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String[] dataStreamPatterns = request.getDataStreamPatterns();
        // TODO hack hack hack
        if (dataStreamPatterns.length == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        RoleDescriptor.IndicesPrivileges[] indicesPrivilegesToCheck = new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices(dataStreamPatterns).privileges(IndexPrivilege.MANAGE.name()).build() };
        HasPrivilegesRequest hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(securityContext.getAuthentication().getEffectiveSubject().getUser().principal());
        hasPrivilegesRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
        hasPrivilegesRequest.indexPrivileges(indicesPrivilegesToCheck);
        hasPrivilegesRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);

        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, ActionListener.wrap(hasPrivilegesResponse -> {
            if (hasPrivilegesResponse.isCompleteMatch()) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                // TODO more detailed failure message
                listener.onFailure(Exceptions.authorizationError("insufficient privileges to configure DLM"));
            }
        }, listener::onFailure));
    }
}
