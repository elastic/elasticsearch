/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.audit.AuditTrailService;

import java.util.Collections;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.xpack.security.audit.AuditUtil.extractRequestId;

public final class ResizeRequestInterceptor implements RequestInterceptor {

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
    public void intercept(RequestInfo requestInfo, AuthorizationEngine authorizationEngine, AuthorizationInfo authorizationInfo,
                          ActionListener<Void> listener) {
        if (requestInfo.getRequest() instanceof ResizeRequest) {
            final ResizeRequest request = (ResizeRequest) requestInfo.getRequest();
            final XPackLicenseState frozenLicenseState = licenseState.copyCurrentLicenseState();
            if (frozenLicenseState.isAuthAllowed()) {
                if (frozenLicenseState.isDocumentAndFieldLevelSecurityAllowed()) {
                    IndicesAccessControl indicesAccessControl =
                        threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                    IndicesAccessControl.IndexAccessControl indexAccessControl =
                        indicesAccessControl.getIndexPermissions(request.getSourceIndex());
                    if (indexAccessControl != null) {
                        final boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                        final boolean dls = indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions();
                        if (fls || dls) {
                            listener.onFailure(new ElasticsearchSecurityException("Resize requests are not allowed for users when " +
                                "field or document level security is enabled on the source index", RestStatus.BAD_REQUEST));
                            return;
                        }
                    }
                }

                authorizationEngine.validateIndexPermissionsAreSubset(requestInfo, authorizationInfo,
                    Collections.singletonMap(request.getSourceIndex(), Collections.singletonList(request.getTargetIndexRequest().index())),
                    wrapPreservingContext(ActionListener.wrap(authzResult -> {
                        if (authzResult.isGranted()) {
                            listener.onResponse(null);
                        } else {
                            if (authzResult.isAuditable()) {
                                auditTrailService.accessDenied(extractRequestId(threadContext), requestInfo.getAuthentication(),
                                    requestInfo.getAction(), request, authorizationInfo);
                            }
                            listener.onFailure(Exceptions.authorizationError("Resizing an index is not allowed when the target index " +
                                "has more permissions than the source index"));
                        }
                    }, listener::onFailure), threadContext));
            } else {
                listener.onResponse(null);
            }
        } else {
            listener.onResponse(null);
        }
    }
}
