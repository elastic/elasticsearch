/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.audit.AuditTrailService;

import static org.elasticsearch.xpack.security.audit.AuditUtil.extractRequestId;

public final class ResizeRequestInterceptor implements RequestInterceptor<ResizeRequest> {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;
    private final AuditTrailService auditTrailService;

    public ResizeRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState,
                                    AuditTrailService auditTrailService) {
        this.threadContext = threadPool.getThreadContext();
        this.licenseState = licenseState;
        this.auditTrailService = auditTrailService;
    }

    @Override
    public void intercept(ResizeRequest request, Authentication authentication, Role userPermissions, String action) {
        final XPackLicenseState frozenLicenseState = licenseState.copyCurrentLicenseState();
        if (frozenLicenseState.isAuthAllowed()) {
            if (frozenLicenseState.isDocumentAndFieldLevelSecurityAllowed()) {
                IndicesAccessControl indicesAccessControl =
                    threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                IndicesAccessControl.IndexAccessControl indexAccessControl =
                    indicesAccessControl.getIndexPermissions(request.getSourceIndex());
                if (indexAccessControl != null) {
                    final boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                    final boolean dls = indexAccessControl.getQueries() != null;
                    if (fls || dls) {
                        throw new ElasticsearchSecurityException("Resize requests are not allowed for users when " +
                            "field or document level security is enabled on the source index", RestStatus.BAD_REQUEST);
                    }
                }
            }

            // ensure that the user would have the same level of access OR less on the target index
            final Automaton sourceIndexPermissions = userPermissions.indices().allowedActionsMatcher(request.getSourceIndex());
            final Automaton targetIndexPermissions =
                userPermissions.indices().allowedActionsMatcher(request.getTargetIndexRequest().index());
            if (Operations.subsetOf(targetIndexPermissions, sourceIndexPermissions) == false) {
                // TODO we've already audited a access granted event so this is going to look ugly
                auditTrailService.accessDenied(extractRequestId(threadContext), authentication, action, request, userPermissions.names());
                throw Exceptions.authorizationError("Resizing an index is not allowed when the target index " +
                    "has more permissions than the source index");
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof ResizeRequest;
    }
}
