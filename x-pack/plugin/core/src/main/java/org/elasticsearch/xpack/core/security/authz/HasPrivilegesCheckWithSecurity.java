/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.HasPrivilegesCheck;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import java.util.Objects;

public class HasPrivilegesCheckWithSecurity implements HasPrivilegesCheck {
    private final SecurityContext securityContext;
    private final Client client;

    @Inject
    public HasPrivilegesCheckWithSecurity(SecurityContext securityContext, Client client) {
        this.securityContext = securityContext;
        this.client = client;
    }

    @Override
    public void checkPrivileges(PrivilegesToCheck privilegesToCheck, ActionListener<Void> listener) {
        Objects.requireNonNull(securityContext.getAuthentication());
        RoleDescriptor.IndicesPrivileges[] indicesPrivilegesToCheck = privilegesToCheck.indexPrivileges()
            .stream()
            .map(
                it -> RoleDescriptor.IndicesPrivileges.builder()
                    .indices(it.indices().toArray(new String[0]))
                    .privileges(it.privileges().toArray(new String[0]))
                    .build()
            )
            .toArray(RoleDescriptor.IndicesPrivileges[]::new);
        String[] clusterPrivilegesToCheck = privilegesToCheck.clusterPrivileges().toArray(new String[0]);

        HasPrivilegesRequest hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(securityContext.getAuthentication().getEffectiveSubject().getUser().principal());
        hasPrivilegesRequest.clusterPrivileges(clusterPrivilegesToCheck);
        hasPrivilegesRequest.indexPrivileges(indicesPrivilegesToCheck);
        hasPrivilegesRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);

        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, ActionListener.wrap(hasPrivilegesResponse -> {
            if (hasPrivilegesResponse.isCompleteMatch()) {
                listener.onResponse(null);
            } else {
                // TODO detailed failure message here
                listener.onFailure(Exceptions.authorizationError("insufficient privileges"));
            }
        }, listener::onFailure));
    }
}
