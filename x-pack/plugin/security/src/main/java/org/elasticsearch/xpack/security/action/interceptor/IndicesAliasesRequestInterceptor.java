/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import java.util.HashMap;
import java.util.Map;

public final class IndicesAliasesRequestInterceptor implements RequestInterceptor<IndicesAliasesRequest> {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;
    private final AuditTrailService auditTrailService;

    public IndicesAliasesRequestInterceptor(ThreadContext threadContext, XPackLicenseState licenseState,
                                            AuditTrailService auditTrailService) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
        this.auditTrailService = auditTrailService;
    }

    @Override
    public void intercept(IndicesAliasesRequest request, Authentication authentication, Role userPermissions, String action) {
        final XPackLicenseState frozenLicenseState = licenseState.copyCurrentLicenseState();
        if (frozenLicenseState.isAuthAllowed()) {
            if (frozenLicenseState.isDocumentAndFieldLevelSecurityAllowed()) {
                IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                for (IndicesAliasesRequest.AliasActions aliasAction : request.getAliasActions()) {
                    if (aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD) {
                        for (String index : aliasAction.indices()) {
                            IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                            if (indexAccessControl != null) {
                                final boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                                final boolean dls = indexAccessControl.getQueries() != null;
                                if (fls || dls) {
                                    throw new ElasticsearchSecurityException("Alias requests are not allowed for users who have " +
                                        "field or document level security enabled on one of the indices", RestStatus.BAD_REQUEST);
                                }
                            }
                        }
                    }
                }
            }

            Map<String, Automaton> permissionsMap = new HashMap<>();
            for (IndicesAliasesRequest.AliasActions aliasAction : request.getAliasActions()) {
                if (aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD) {
                    for (String index : aliasAction.indices()) {
                        Automaton indexPermissions =
                            permissionsMap.computeIfAbsent(index, userPermissions.indices()::allowedActionsMatcher);
                        for (String alias : aliasAction.aliases()) {
                            Automaton aliasPermissions =
                                permissionsMap.computeIfAbsent(alias, userPermissions.indices()::allowedActionsMatcher);
                            if (Operations.subsetOf(aliasPermissions, indexPermissions) == false) {
                                // TODO we've already audited a access granted event so this is going to look ugly
                                auditTrailService.accessDenied(AuditUtil.extractRequestId(threadContext), authentication, action, request,
                                    userPermissions.names());
                                throw Exceptions.authorizationError("Adding an alias is not allowed when the alias " +
                                    "has more permissions than any of the indices");
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof IndicesAliasesRequest;
    }
}
