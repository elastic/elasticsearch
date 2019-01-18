/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.IndicesAndAliasesResolver.ResolvedIndices;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface AuthorizationEngine {

    void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener);

    void authorizeRunAs(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener);

    void authorizeClusterAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener);

    void authorizeIndexAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                              AsyncSupplier<ResolvedIndices> indicesAsyncSupplier, Function<String, AliasOrIndex> aliasOrIndexFunction,
                              ActionListener<IndexAuthorizationResult> listener);

    void loadAuthorizedIndices(RequestInfo requestInfo, AuthorizationInfo info,
                               Map<String, AliasOrIndex> aliasAndIndexLookup, ActionListener<List<String>> listener);

    interface AuthorizationInfo {

        Map<String, Object> asMap();

        default AuthorizationInfo getAuthenticatedUserAuthorizationInfo() {
            return this;
        }
    }

    final class EmptyAuthorizationInfo implements AuthorizationInfo {

        public static final EmptyAuthorizationInfo INSTANCE = new EmptyAuthorizationInfo();

        private EmptyAuthorizationInfo() {}

        @Override
        public Map<String, Object> asMap() {
            return Collections.emptyMap();
        }
    }

    final class RequestInfo {

        private final Authentication authentication;
        private final TransportRequest request;
        private final String action;

        public RequestInfo(Authentication authentication, TransportRequest request, String action) {
            this.authentication = authentication;
            this.request = request;
            this.action = action;
        }

        public String getAction() {
            return action;
        }

        public Authentication getAuthentication() {
            return authentication;
        }

        public TransportRequest getRequest() {
            return request;
        }
    }

    class AuthorizationResult {

        private final boolean granted;
        private final boolean auditable;

        public AuthorizationResult(boolean granted) {
            this(granted, true);
        }

        public AuthorizationResult(boolean granted, boolean auditable) {
            this.granted = granted;
            this.auditable = auditable;
        }

        public boolean isGranted() {
            return granted;
        }

        public boolean isAuditable() {
            return auditable;
        }

        public static AuthorizationResult granted() {
            return new AuthorizationResult(true);
        }

        public static AuthorizationResult deny() {
            return new AuthorizationResult(false);
        }
    }

    class IndexAuthorizationResult extends AuthorizationResult {

        private final IndicesAccessControl indicesAccessControl;

        IndexAuthorizationResult(boolean auditable, IndicesAccessControl indicesAccessControl) {
            super(indicesAccessControl == null || indicesAccessControl.isGranted(), auditable);
            this.indicesAccessControl = indicesAccessControl;
        }

        public IndicesAccessControl getIndicesAccessControl() {
            return indicesAccessControl;
        }
    }

    @FunctionalInterface
    interface AsyncSupplier<V> {

        void get(ActionListener<V> listener);
    }
}
