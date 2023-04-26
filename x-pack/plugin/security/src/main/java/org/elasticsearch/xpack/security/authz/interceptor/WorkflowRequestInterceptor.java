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
import org.elasticsearch.rest.RestStatus;
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

public class WorkflowRequestInterceptor implements RequestInterceptor {

    private static final Logger logger = LogManager.getLogger(WorkflowRequestInterceptor.class);

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public WorkflowRequestInterceptor(ThreadContext threadContext, XPackLicenseState licenseState) {
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
        final Role role = RBACEngine.maybeGetRBACEngineRole(threadContext.getTransient(AUTHORIZATION_INFO_KEY));
        if (role != null && role.hasWorkflowsPermission()) {
            String restEndpoint = threadContext.getHeader("_xpack_rest_handler_name");
            logger.info("workflows: intercepting request for endpoint [" + restEndpoint + "]");
            if(restEndpoint != null) {
                boolean isWorkflowEndpointAllowed = role.checkWorkflowEndpoint(restEndpoint);
                logger.info("workflows: access to endpoint [" + restEndpoint + "] is [" + (isWorkflowEndpointAllowed ? "allowed" : "not allowed") + "]");
                if (false == isWorkflowEndpointAllowed) {
                    listener.onFailure(new ElasticsearchSecurityException("access to endpoint not allowed", RestStatus.FORBIDDEN));
                    return;
                }
            } else {
                logger.info("workflows: access not allowed because request not originating from rest handler and user has restriction to workflows " + role.workflows().workflows() + "");
                listener.onFailure(new ElasticsearchSecurityException("access not allowed", RestStatus.FORBIDDEN));
                return;
            }
        }
        listener.onResponse(null);
    }
}
