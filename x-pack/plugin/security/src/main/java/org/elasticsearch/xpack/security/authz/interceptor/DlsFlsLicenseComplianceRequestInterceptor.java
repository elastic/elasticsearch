/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.RBACEngine;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_KEY;

public class DlsFlsLicenseComplianceRequestInterceptor implements RequestInterceptor {
    private static final Logger logger = LogManager.getLogger(DlsFlsLicenseComplianceRequestInterceptor.class);

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public DlsFlsLicenseComplianceRequestInterceptor(ThreadContext threadContext, XPackLicenseState licenseState) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
    }

    @Override
    public void intercept(
        AuthorizationEngine.RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo,
        ActionListener<Void> listener
    ) {

        if (requestInfo.getRequest() instanceof IndicesRequest && false == TransportActionProxy.isProxyAction(requestInfo.getAction())) {
            if (false == licenseState.isAllowed(XPackLicenseState.Feature.SECURITY_DLS_FLS)) {
                final Role role = RBACEngine.maybeGetRBACEngineRole(threadContext.getTransient(AUTHORIZATION_INFO_KEY));
                if (role == null || role.hasFieldOrDocumentLevelSecurity()) {
                    logger.trace(
                        "Role has DLS or FLS and license is incompatible. "
                            + "Checking for whether the request touches any indices that have DLS or FLS configured"
                    );
                    final IndicesAccessControl indicesAccessControl = threadContext.getTransient(INDICES_PERMISSIONS_KEY);
                    if (indicesAccessControl != null && indicesAccessControl.hasFieldOrDocumentLevelSecurity()) {
                        final ElasticsearchSecurityException licenseException = LicenseUtils.newComplianceException(
                            "field and document level security"
                        );
                        licenseException.addMetadata(
                            "es.indices_with_dls_or_fls",
                            indicesAccessControl.getIndicesWithFieldOrDocumentLevelSecurity()
                        );
                        listener.onFailure(licenseException);
                        return;
                    }
                }
            }
        }
        listener.onResponse(null);
    }
}
