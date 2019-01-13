/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

/**
 * Base class for interceptors that disables features when field level security is configured for indices a request
 * is going to execute on.
 */
abstract class FieldAndDocumentLevelSecurityRequestInterceptor<Request extends IndicesRequest> implements
        RequestInterceptor<Request> {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;
    private final Logger logger;

    FieldAndDocumentLevelSecurityRequestInterceptor(ThreadContext threadContext, XPackLicenseState licenseState) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public void intercept(Request request, Authentication authentication, Role userPermissions, String action) {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
            final IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            for (String index : request.indices()) {
                IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                if (indexAccessControl != null) {
                    boolean fieldLevelSecurityEnabled = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                    boolean documentLevelSecurityEnabled = indexAccessControl.getQueries() != null;
                    if (fieldLevelSecurityEnabled || documentLevelSecurityEnabled) {
                        if (fieldLevelSecurityEnabled || documentLevelSecurityEnabled) {
                            logger.trace("intercepted request for index [{}] with field level access controls [{}] document level access " +
                                    "controls [{}]. disabling conflicting features", index, fieldLevelSecurityEnabled,
                                documentLevelSecurityEnabled);
                        }
                        disableFeatures(request, fieldLevelSecurityEnabled, documentLevelSecurityEnabled);
                        return;
                    }
                }
                logger.trace("intercepted request for index [{}] without field or document level access controls", index);
            }
        }
    }

    protected abstract void disableFeatures(Request request, boolean fieldLevelSecurityEnabled, boolean documentLevelSecurityEnabled);

}
