/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.core.security.action.dlm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.PrivilegesCheckRequest;
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

    private final Client client;
    private final SecurityContext securityContext;

    @Inject
    public TransportAuthorizeDataLifecycleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SecurityContext securityContext,
        Client client
    ) {
        super(AuthorizeDataLifecycleAction.NAME, transportService, actionFilters, AuthorizeDataLifecycleAction.Request::new);
        this.client = client;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(Task task, AuthorizeDataLifecycleAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        PrivilegesCheckRequest.CorePrivilegesToCheck privilegesToCheck = ((PrivilegesCheckRequest) request).getPrivilegesToCheck();
        // TODO hack hack hack
        if (privilegesToCheck == null) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        String[] indices = privilegesToCheck.indices();
        RoleDescriptor.IndicesPrivileges[] indicesPrivilegesToCheck = new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices(indices).privileges(IndexPrivilege.MANAGE.name()).build() };

        HasPrivilegesRequest hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(securityContext.getAuthentication().getEffectiveSubject().getUser().principal());
        hasPrivilegesRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
        hasPrivilegesRequest.indexPrivileges(indicesPrivilegesToCheck);
        hasPrivilegesRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);

        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, ActionListener.wrap(response -> {
            if (response.isCompleteMatch()) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                // TODO more detailed failure message
                listener.onFailure(Exceptions.authorizationError("insufficient privileges to configure data lifecycle"));
            }

        }, listener::onFailure));
    }
}
