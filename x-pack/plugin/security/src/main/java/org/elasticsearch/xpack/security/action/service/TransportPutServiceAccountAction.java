/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.PutServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutServiceAccountResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.service.ElasticServiceAccounts;
import org.elasticsearch.xpack.security.authc.service.IndexUserServiceAccountStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportPutServiceAccountAction extends HandledTransportAction<PutServiceAccountRequest, PutServiceAccountResponse> {

    private final SecurityContext securityContext;
    private final AuthorizationService authorizationService;
    private final NativePrivilegeStore privilegeStore;
    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportPutServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SecurityContext securityContext,
        AuthorizationService authorizationService,
        NativePrivilegeStore privilegeStore,
        ServiceAccountService serviceAccountService
    ) {
        super(
            PutServiceAccountAction.NAME,
            transportService,
            actionFilters,
            PutServiceAccountRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.securityContext = securityContext;
        this.authorizationService = authorizationService;
        this.privilegeStore = privilegeStore;
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(Task task, PutServiceAccountRequest request, ActionListener<PutServiceAccountResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required to create a service account"));
            return;
        }

        if (ElasticServiceAccounts.NAMESPACE.equals(request.getNamespace())) {
            listener.onFailure(
                new IllegalArgumentException(
                    "namespace [" + ElasticServiceAccounts.NAMESPACE + "] is reserved for built-in service accounts"
                )
            );
            return;
        }

        final RoleDescriptor roleDescriptor = request.getRoleDescriptor();
        final ElasticsearchSecurityException pocViolation = rejectUnsupportedRoleFeatures(roleDescriptor);
        if (pocViolation != null) {
            listener.onFailure(pocViolation);
            return;
        }

        final IndexUserServiceAccountStore store = serviceAccountService.getIndexUserServiceAccountStore();
        if (store == null) {
            listener.onFailure(new IllegalStateException("user-defined service account store is not configured on this node"));
            return;
        }

        final AuthorizationEngine.PrivilegesToCheck privilegesToCheck = new AuthorizationEngine.PrivilegesToCheck(
            roleDescriptor.getClusterPrivileges(),
            roleDescriptor.getIndicesPrivileges(),
            roleDescriptor.getApplicationPrivileges(),
            false
        );

        final Set<String> applicationNames = Arrays.stream(roleDescriptor.getApplicationPrivileges())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());

        privilegeStore.getPrivileges(applicationNames, null, ActionListener.wrap(applicationPrivilegeDescriptors -> {
            authorizationService.checkPrivileges(
                authentication.getEffectiveSubject(),
                privilegesToCheck,
                applicationPrivilegeDescriptors,
                ActionListener.wrap(privilegesCheckResult -> {
                    if (privilegesCheckResult.allChecksSuccess() == false) {
                        listener.onFailure(
                            new ElasticsearchSecurityException(
                                "caller does not have all privileges in the requested role_descriptor; "
                                    + "the calling user must hold every privilege granted to the new service account",
                                RestStatus.FORBIDDEN
                            )
                        );
                        return;
                    }
                    store.putAccount(authentication, request, listener.map(PutServiceAccountResponse::new));
                }, listener::onFailure)
            );
        }, listener::onFailure));
    }

    private static ElasticsearchSecurityException rejectUnsupportedRoleFeatures(RoleDescriptor roleDescriptor) {
        if (roleDescriptor.getRunAs().length > 0) {
            return new ElasticsearchSecurityException(
                "role_descriptor with [run_as] is not supported for user-defined service accounts in this POC",
                RestStatus.BAD_REQUEST
            );
        }
        for (RoleDescriptor.IndicesPrivileges indices : roleDescriptor.getIndicesPrivileges()) {
            if (indices.isUsingDocumentLevelSecurity()) {
                return new ElasticsearchSecurityException(
                    "role_descriptor with document level security is not supported for user-defined service accounts in this POC",
                    RestStatus.BAD_REQUEST
                );
            }
            if (indices.isUsingFieldLevelSecurity()) {
                return new ElasticsearchSecurityException(
                    "role_descriptor with field level security is not supported for user-defined service accounts in this POC",
                    RestStatus.BAD_REQUEST
                );
            }
            for (String pattern : indices.getIndices()) {
                if (pattern.indexOf(':') >= 0) {
                    return new ElasticsearchSecurityException(
                        "role_descriptor index pattern ["
                            + pattern
                            + "] contains ':' which is not supported for user-defined service accounts in this POC",
                        RestStatus.BAD_REQUEST
                    );
                }
            }
        }
        if (roleDescriptor.hasRemoteIndicesPrivileges() || roleDescriptor.hasRemoteClusterPermissions()) {
            return new ElasticsearchSecurityException(
                "role_descriptor with remote indices or remote cluster privileges is not supported for user-defined service accounts in this POC",
                RestStatus.BAD_REQUEST
            );
        }
        return null;
    }
}
