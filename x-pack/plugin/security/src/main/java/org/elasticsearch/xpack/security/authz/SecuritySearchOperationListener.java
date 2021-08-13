/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;

/**
 * A {@link SearchOperationListener} that is used to provide authorization for scroll requests.
 * <p>
 * In order to identify the user associated with a scroll request, we replace the {@link ReaderContext}
 * on creation with a custom implementation that holds the {@link Authentication} object. When
 * this context is accessed again in {@link SearchOperationListener#onPreQueryPhase(SearchContext)}
 * the ScrollContext is inspected for the authentication, which is compared to the currently
 * authentication.
 */
public final class SecuritySearchOperationListener implements SearchOperationListener {

    private final SecurityContext securityContext;
    private final AuditTrailService auditTrailService;

    public SecuritySearchOperationListener(SecurityContext securityContext, AuditTrailService auditTrail) {
        this.securityContext = securityContext;
        this.auditTrailService = auditTrail;
    }

    /**
     * Adds the {@link Authentication} to the {@link ScrollContext}
     */
    @Override
    public void onNewScrollContext(ReaderContext readerContext) {
        readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, securityContext.getAuthentication());
        // store the DLS and FLS permissions of the initial search request that created the scroll
        // this is then used to assert the DLS/FLS permission for the scroll search action
        IndicesAccessControl indicesAccessControl =
                securityContext.getThreadContext().getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        assert indicesAccessControl != null : "thread context does not contain index access control";
        readerContext.putInContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
    }

    /**
     * Checks for the {@link ReaderContext} if it exists and compares the {@link Authentication}
     * object from the scroll context with the current authentication context
     */
    @Override
    public void validateReaderContext(ReaderContext readerContext, TransportRequest request) {
        if (readerContext.scrollContext() != null) {
            final Authentication originalAuth = readerContext.getFromContext(AuthenticationField.AUTHENTICATION_KEY);
            final Authentication current = securityContext.getAuthentication();
            final ThreadContext threadContext = securityContext.getThreadContext();
            final String action = threadContext.getTransient(ORIGINATING_ACTION_KEY);
            ensureAuthenticatedUserIsSame(originalAuth, current, auditTrailService, readerContext.id(), action, request,
                    AuditUtil.extractRequestId(threadContext), threadContext.getTransient(AUTHORIZATION_INFO_KEY));
            // piggyback on context validation to assert the DLS/FLS permissions on the thread context of the scroll search handler
            if (null == securityContext.getThreadContext().getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY)) {
                // fill in the DLS and FLS permissions for the scroll search action from the scroll context
                IndicesAccessControl scrollIndicesAccessControl =
                        readerContext.getFromContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                assert scrollIndicesAccessControl != null : "scroll does not contain index access control";
                securityContext.getThreadContext().putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY,
                        scrollIndicesAccessControl);
            }
        }
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        ensureIndicesAccessControlForScrollThreadContext(searchContext);
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        ensureIndicesAccessControlForScrollThreadContext(searchContext);
    }

    void ensureIndicesAccessControlForScrollThreadContext(SearchContext searchContext) {
        if (searchContext.readerContext().scrollContext() != null) {
            IndicesAccessControl threadIndicesAccessControl =
                    securityContext.getThreadContext().getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            if (null == threadIndicesAccessControl) {
                throw new ElasticsearchSecurityException("Unexpected null indices access control for search context [" +
                        searchContext.id() + "] for request [" + searchContext.request().getDescription() + "] with source [" +
                        searchContext.source() + "]");
            }
        }
    }

    /**
     * Compares the {@link Authentication} that was stored in the {@link ReaderContext} with the
     * current authentication. We cannot guarantee that all of the details of the authentication will
     * be the same. Some things that could differ include the roles, the name of the authenticating
     * (or lookup) realm. To work around this we compare the username and the originating realm type.
     */
    static void ensureAuthenticatedUserIsSame(Authentication original, Authentication current, AuditTrailService auditTrailService,
                                              ShardSearchContextId id, String action, TransportRequest request, String requestId,
                                              AuthorizationInfo authorizationInfo) {
        final boolean sameUser = original.canAccessResourcesOf(current);
        if (sameUser == false) {
            auditTrailService.get().accessDenied(requestId, current, action, request, authorizationInfo);
            throw new SearchContextMissingException(id);
        }
    }
}
