/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.MemoizedSupplier;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.SortedMap;
import java.util.TreeMap;

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
                var licenseChecker = new MemoizedSupplier<>(() -> licenseState.checkFeature(Feature.SECURITY_DLS_FLS));
                final IndicesAccessControl indicesAccessControl
                    = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                final SortedMap<String, IndicesAccessControl.IndexAccessControl> accessControlByIndex = new TreeMap<>();
                for (String index : requestIndices(indicesRequest)) {
                    IndicesAccessControl.IndexAccessControl accessControl = indicesAccessControl.getIndexPermissions(index);
                    if (accessControl != null) {
                        final boolean fieldLevelSecurityEnabled = accessControl.getFieldPermissions().hasFieldLevelSecurity();
                        final boolean documentLevelSecurityEnabled = accessControl.getDocumentPermissions().hasDocumentLevelPermissions();
                        if ((fieldLevelSecurityEnabled || documentLevelSecurityEnabled) && licenseChecker.get()) {
                            logger.trace("intercepted request for index [{}] with field level access controls [{}] " +
                                "document level access controls [{}]. disabling conflicting features",
                                index, fieldLevelSecurityEnabled, documentLevelSecurityEnabled);
                            accessControlByIndex.put(index, accessControl);
                        }
                    }
                    logger.trace("intercepted request for index [{}] without field or document level access controls", index);
                }
                if (accessControlByIndex.isEmpty() == false) {
                    disableFeatures(indicesRequest, accessControlByIndex, listener);
                    return;
                }
            }
        }
        listener.onResponse(null);
    }

    abstract void disableFeatures(IndicesRequest indicesRequest,
                                  SortedMap<String, IndicesAccessControl.IndexAccessControl> accessControlByIndex,
                                  ActionListener<Void> listener);

    String[] requestIndices(IndicesRequest indicesRequest) {
        return indicesRequest.indices();
    }

    abstract boolean supports(IndicesRequest request);
}
