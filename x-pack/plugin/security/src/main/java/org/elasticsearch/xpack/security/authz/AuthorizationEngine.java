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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public interface AuthorizationEngine {

    void resolveAuthorizationInfo(Authentication authentication, TransportRequest request, String action,
                                  ActionListener<AuthorizationInfo> listener);

    void authorizeRunAs(Authentication authentication, TransportRequest request, String action, AuthorizationInfo authorizationInfo,
                        ActionListener<AuthorizationResult> listener);

    void authorizeClusterAction(Authentication authentication, TransportRequest request, String action, AuthorizationInfo authorizationInfo,
                                ActionListener<AuthorizationResult> listener);

    boolean checkSameUserPermissions(String action, TransportRequest request, Authentication authentication);

    boolean shouldAuthorizeIndexActionNameOnly(String action, TransportRequest request);

    void authorizeIndexActionName(Authentication authentication, TransportRequest request, String action,
                                  AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener);

    List<String> loadAuthorizedIndices(Authentication authentication, TransportRequest request, String action, AuthorizationInfo info,
                                       Map<String, AliasOrIndex> aliasAndIndexLookup);

    void buildIndicesAccessControl(Authentication authentication, TransportRequest request, String action,
                                   AuthorizationInfo authorizationInfo, Set<String> indices,
                                   SortedMap<String, AliasOrIndex> aliasAndIndexLookup, ActionListener<IndexAuthorizationResult> listener);

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
            super(indicesAccessControl.isGranted(), auditable);
            this.indicesAccessControl = indicesAccessControl;
        }

        public IndicesAccessControl getIndicesAccessControl() {
            return indicesAccessControl;
        }
    }
}
