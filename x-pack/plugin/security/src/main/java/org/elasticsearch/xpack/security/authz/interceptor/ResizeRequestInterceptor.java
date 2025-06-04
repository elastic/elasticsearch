/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;

import java.util.Collections;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.security.audit.AuditUtil.extractRequestId;

public final class ResizeRequestInterceptor implements RequestInterceptor {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;
    private final AuditTrailService auditTrailService;
    private final boolean dlsFlsEnabled;

    public ResizeRequestInterceptor(
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        AuditTrailService auditTrailService,
        boolean dlsFlsEnabled
    ) {
        this.threadContext = threadPool.getThreadContext();
        this.licenseState = licenseState;
        this.auditTrailService = auditTrailService;
        this.dlsFlsEnabled = dlsFlsEnabled;
    }

    @Override
    public SubscribableListener<Void> intercept(
        RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof ResizeRequest request) {
            final AuditTrail auditTrail = auditTrailService.get();
            final boolean isDlsLicensed = DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
            final boolean isFlsLicensed = FIELD_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
            if (dlsFlsEnabled && (isDlsLicensed || isFlsLicensed)) {
                IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(
                    request.getSourceIndex()
                );
                if (indexAccessControl != null
                    && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                        || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions())) {
                    return SubscribableListener.newFailed(
                        new ElasticsearchSecurityException(
                            "Resize requests are not allowed for users when "
                                + "field or document level security is enabled on the source index",
                            RestStatus.BAD_REQUEST
                        )
                    );
                }
            }

            final SubscribableListener<Void> listener = new SubscribableListener<>();
            authorizationEngine.validateIndexPermissionsAreSubset(
                requestInfo,
                authorizationInfo,
                Collections.singletonMap(request.getSourceIndex(), Collections.singletonList(request.getTargetIndexRequest().index())),
                wrapPreservingContext(ActionListener.wrap(authzResult -> {
                    if (authzResult.isGranted()) {
                        listener.onResponse(null);
                    } else {
                        auditTrail.accessDenied(
                            extractRequestId(threadContext),
                            requestInfo.getAuthentication(),
                            requestInfo.getAction(),
                            request,
                            authorizationInfo
                        );
                        listener.onFailure(
                            Exceptions.authorizationError(
                                "Resizing an index is not allowed when the target index " + "has more permissions than the source index"
                            )
                        );
                    }
                }, listener::onFailure), threadContext)
            );
            return listener;
        } else {
            return SubscribableListener.nullSuccess();
        }
    }
}
