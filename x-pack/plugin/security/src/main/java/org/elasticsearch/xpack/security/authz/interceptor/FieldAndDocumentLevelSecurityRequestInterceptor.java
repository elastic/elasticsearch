/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

/**
 * Base class for interceptors that disables features when field level security is configured for indices a request
 * is going to execute on.
 */
abstract class FieldAndDocumentLevelSecurityRequestInterceptor implements RequestInterceptor {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;
    private final Logger logger;

    FieldAndDocumentLevelSecurityRequestInterceptor(ThreadContext threadContext, XPackLicenseState licenseState) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public void intercept(RequestInfo requestInfo, AuthorizationEngine authorizationEngine, AuthorizationInfo authorizationInfo,
                          ActionListener<Void> listener) {
        if (requestInfo.getRequest() instanceof IndicesRequest) {
            IndicesRequest indicesRequest = (IndicesRequest) requestInfo.getRequest();
            boolean shouldIntercept = licenseState.isSecurityEnabled() && licenseState.isAllowed(Feature.SECURITY_DLS_FLS);
            if (supports(indicesRequest) && shouldIntercept) {
                final IndicesAccessControl indicesAccessControl =
                    threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                for (String index : indicesRequest.indices()) {
                    IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                    if (indexAccessControl != null) {
                        boolean fieldLevelSecurityEnabled = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                        boolean documentLevelSecurityEnabled = indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions();
                        if (fieldLevelSecurityEnabled || documentLevelSecurityEnabled) {
                            logger.trace("intercepted request for index [{}] with field level access controls [{}] " +
                                "document level access controls [{}]. disabling conflicting features",
                                index, fieldLevelSecurityEnabled, documentLevelSecurityEnabled);
                            disableFeatures(indicesRequest, fieldLevelSecurityEnabled, documentLevelSecurityEnabled, listener);
                            return;
                        }
                    }
                    logger.trace("intercepted request for index [{}] without field or document level access controls", index);
                }
            }
        }
        listener.onResponse(null);
    }

    abstract void disableFeatures(IndicesRequest request, boolean fieldLevelSecurityEnabled, boolean documentLevelSecurityEnabled,
                                  ActionListener<Void> listener);

    abstract boolean supports(IndicesRequest request);
}
