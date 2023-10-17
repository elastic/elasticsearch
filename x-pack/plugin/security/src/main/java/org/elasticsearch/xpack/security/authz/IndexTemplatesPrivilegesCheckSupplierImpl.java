/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexTemplatesPrivilegesCheckSupplier;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class IndexTemplatesPrivilegesCheckSupplierImpl implements IndexTemplatesPrivilegesCheckSupplier {
    private final AuthorizationService authorizationService;
    private final SecurityContext securityContext;

    @Inject
    public IndexTemplatesPrivilegesCheckSupplierImpl(AuthorizationService authorizationService, SecurityContext securityContext) {
        this.authorizationService = authorizationService;
        this.securityContext = securityContext;
    }

    @Override
    public void getPrivilegesCheckForIndexPatterns(ActionListener<Consumer<List<String>>> listener) {
        Objects.requireNonNull(securityContext.getAuthentication());
        Subject subject = securityContext.getAuthentication().getEffectiveSubject();
        // Might need to resolve this instead
        AuthorizationEngine.AuthorizationInfo authzInfo = securityContext.getAuthorizationInfoFromContext();
        listener.onResponse(indexPatterns -> {
            // We could instead check cluster and index privileges in one go, and check details to determine the authz result
            final AuthorizationEngine.PrivilegesCheckResult hasClusterPrivilege = authorizationService.checkPrivileges(
                subject,
                authzInfo,
                new AuthorizationEngine.PrivilegesToCheck(
                    new String[] { "manage_index_templates" },
                    new RoleDescriptor.IndicesPrivileges[0],
                    new RoleDescriptor.ApplicationResourcePrivileges[0],
                    false
                ),
                Collections.emptyList()
            );
            // Cluster privilege means always grant, so no need to check index privileges
            if (hasClusterPrivilege.allChecksSuccess()) {
                return;
            }

            final RoleDescriptor.IndicesPrivileges[] indicesPrivilegesToCheck = new RoleDescriptor.IndicesPrivileges[] {
                // Using `manage` as a placeholder; in the final implementation this will be `manage_index_templates`
                RoleDescriptor.IndicesPrivileges.builder().indices(indexPatterns.toArray(new String[0])).privileges("manage").build() };
            final AuthorizationEngine.PrivilegesCheckResult hasIndexPrivileges = authorizationService.checkPrivileges(
                subject,
                authzInfo,
                new AuthorizationEngine.PrivilegesToCheck(
                    new String[0],
                    indicesPrivilegesToCheck,
                    new RoleDescriptor.ApplicationResourcePrivileges[0],
                    false
                ),
                Collections.emptyList()
            );
            if (false == hasIndexPrivileges.allChecksSuccess()) {
                throw Exceptions.authorizationError("insufficient privileges");
            }
        });
    }
}
