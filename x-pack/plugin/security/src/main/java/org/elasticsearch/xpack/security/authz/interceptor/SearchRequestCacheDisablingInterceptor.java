/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Arrays;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;

public class SearchRequestCacheDisablingInterceptor implements RequestInterceptor {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public SearchRequestCacheDisablingInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        this.threadContext = threadPool.getThreadContext();
        this.licenseState = licenseState;
    }

    @Override
    public SubscribableListener<Void> intercept(
        AuthorizationEngine.RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationEngine.AuthorizationInfo authorizationInfo
    ) {
        final boolean isDlsLicensed = DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
        final boolean isFlsLicensed = FIELD_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
        if (requestInfo.getRequest() instanceof SearchRequest searchRequest
            && false == TransportActionProxy.isProxyAction(requestInfo.getAction())
            && hasRemoteIndices(searchRequest)
            && (isDlsLicensed || isFlsLicensed)) {
            final IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            if (indicesAccessControl.getFieldAndDocumentLevelSecurityUsage() != IndicesAccessControl.DlsFlsUsage.NONE) {
                searchRequest.requestCache(false);
            }
        }
        return SubscribableListener.nullSuccess();
    }

    // package private for test
    static boolean hasRemoteIndices(SearchRequest request) {
        return Arrays.stream(request.indices()).anyMatch(RemoteClusterAware::isRemoteIndexName);
    }
}
