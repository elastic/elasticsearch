/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.HasPrivilegesCheck;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import java.util.Objects;
import java.util.function.Consumer;

public class HasPrivilegesCheckWithSecurity implements HasPrivilegesCheck {
    private final AuthorizationService authorizationService;
    private final SecurityContext securityContext;

    @Inject
    public HasPrivilegesCheckWithSecurity(AuthorizationService authorizationService, SecurityContext securityContext) {
        this.authorizationService = authorizationService;
        this.securityContext = securityContext;
    }

    @Override
    public void getPrivilegesCheck(ActionListener<Consumer<PrivilegesToCheck>> listener) {
        Objects.requireNonNull(securityContext.getAuthentication());
        authorizationService.getPrivilegesCheck(
            securityContext.getAuthentication().getEffectiveSubject(),
            ActionListener.wrap(privilegesCheck -> listener.onResponse(privilegesToCheck -> {
                final RoleDescriptor.IndicesPrivileges[] indicesPrivilegesToCheck = privilegesToCheck.indexPrivileges()
                    .stream()
                    .map(
                        it -> RoleDescriptor.IndicesPrivileges.builder()
                            .indices(it.indices().toArray(new String[0]))
                            .privileges(it.privileges().toArray(new String[0]))
                            .build()
                    )
                    .toArray(RoleDescriptor.IndicesPrivileges[]::new);
                final String[] clusterPrivilegesToCheck = privilegesToCheck.clusterPrivileges().toArray(new String[0]);
                final AuthorizationEngine.PrivilegesCheckResult checkResult = privilegesCheck.apply(
                    new AuthorizationEngine.PrivilegesToCheck(
                        clusterPrivilegesToCheck,
                        indicesPrivilegesToCheck,
                        new RoleDescriptor.ApplicationResourcePrivileges[0],
                        false
                    )
                );
                if (false == checkResult.allChecksSuccess()) {
                    throw Exceptions.authorizationError("insufficient privileges");
                }
            }), listener::onFailure)
        );
    }
}
