/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.RBACEngine;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_KEY;

public class DlsFlsLicenseRequestInterceptor implements RequestInterceptor {
    private static final Logger logger = LogManager.getLogger(DlsFlsLicenseRequestInterceptor.class);

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public DlsFlsLicenseRequestInterceptor(ThreadContext threadContext, XPackLicenseState licenseState) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
    }

    @Override
    public SubscribableListener<Void> intercept(
        AuthorizationEngine.RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof IndicesRequest && false == TransportActionProxy.isProxyAction(requestInfo.getAction())) {
            final Role role = RBACEngine.maybeGetRBACEngineRole(threadContext.getTransient(AUTHORIZATION_INFO_KEY));
            // Checking whether role has FLS or DLS first before checking indicesAccessControl for efficiency because indicesAccessControl
            // can contain a long list of indices
            // But if role is null, it means a custom authorization engine is in use and we have to directly go check indicesAccessControl
            if (role == null || role.hasFieldOrDocumentLevelSecurity()) {
                logger.trace("Role has DLS or FLS. Checking for whether the request touches any indices that have DLS or FLS configured");
                final IndicesAccessControl indicesAccessControl = threadContext.getTransient(INDICES_PERMISSIONS_KEY);
                if (indicesAccessControl != null) {
                    final XPackLicenseState frozenLicenseState = licenseState.copyCurrentLicenseState();
                    if (logger.isDebugEnabled()) {
                        final IndicesAccessControl.DlsFlsUsage dlsFlsUsage = indicesAccessControl.getFieldAndDocumentLevelSecurityUsage();
                        if (dlsFlsUsage.hasFieldLevelSecurity()) {
                            logger.debug(
                                () -> format(
                                    "User [%s] has field level security on [%s]",
                                    requestInfo.getAuthentication(),
                                    indicesAccessControl.getIndicesWithFieldLevelSecurity()
                                )
                            );
                        }
                        if (dlsFlsUsage.hasDocumentLevelSecurity()) {
                            logger.debug(
                                () -> format(
                                    "User [%s] has document level security on [%s]",
                                    requestInfo.getAuthentication(),
                                    indicesAccessControl.getIndicesWithDocumentLevelSecurity()
                                )
                            );
                        }
                    }
                    if (false == DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(frozenLicenseState)
                        || false == FIELD_LEVEL_SECURITY_FEATURE.checkWithoutTracking(frozenLicenseState)) {
                        boolean incompatibleLicense = false;
                        IndicesAccessControl.DlsFlsUsage dlsFlsUsage = indicesAccessControl.getFieldAndDocumentLevelSecurityUsage();
                        if (dlsFlsUsage.hasDocumentLevelSecurity() && false == DOCUMENT_LEVEL_SECURITY_FEATURE.check(frozenLicenseState)) {
                            incompatibleLicense = true;
                        }
                        if (dlsFlsUsage.hasFieldLevelSecurity() && false == FIELD_LEVEL_SECURITY_FEATURE.check(frozenLicenseState)) {
                            incompatibleLicense = true;
                        }

                        if (incompatibleLicense) {
                            final ElasticsearchSecurityException licenseException = LicenseUtils.newComplianceException(
                                "field and document level security"
                            );
                            licenseException.addMetadata(
                                "es.indices_with_dls_or_fls",
                                indicesAccessControl.getIndicesWithFieldOrDocumentLevelSecurity()
                            );
                            return SubscribableListener.newFailed(licenseException);
                        }
                    }
                }
            }
        }
        return SubscribableListener.nullSuccess();
    }
}
