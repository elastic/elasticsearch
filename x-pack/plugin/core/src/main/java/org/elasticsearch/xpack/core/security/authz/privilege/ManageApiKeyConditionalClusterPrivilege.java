/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Conditional cluster privilege for managing API keys
 */
public final class ManageApiKeyConditionalClusterPrivilege implements ConditionalClusterPrivilege {

    private static final String MANAGE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/*";
    private static final String CREATE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/create";
    private static final String GET_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/get";
    private static final String INVALIDATE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/invalidate";
    private static final List<String> API_KEY_ACTION_PATTERNS = List.of(MANAGE_API_KEY_PATTERN, CREATE_API_KEY_PATTERN, GET_API_KEY_PATTERN,
            INVALIDATE_API_KEY_PATTERN);

    private final boolean restrictActionsToAuthenticatedUser;
    private final ClusterPrivilege privilege;
    private final BiPredicate<TransportRequest, Authentication> requestPredicate;

    /**
     * Constructor for {@link ManageApiKeyConditionalClusterPrivilege}
     *
     * @param actions set of API key cluster actions
     * @param restrictActionsToAuthenticatedUser if {@code true} privileges will be restricted to current authenticated user.
     */
    public ManageApiKeyConditionalClusterPrivilege(Set<String> actions, boolean restrictActionsToAuthenticatedUser) {
        // validate allowed actions
        for (String action : actions) {
            if (ClusterPrivilege.MANAGE_API_KEY.predicate().test(action) == false) {
                throw new IllegalArgumentException("invalid action [ " + action + " ] specified, expected API key privilege actions from [ "
                        + API_KEY_ACTION_PATTERNS + " ]");
            }
        }
        this.privilege = ClusterPrivilege.get(actions).v1();
        this.restrictActionsToAuthenticatedUser = restrictActionsToAuthenticatedUser;

        this.requestPredicate = (request, authentication) -> {
            if (request instanceof CreateApiKeyRequest) {
                return true;
            } else if (request instanceof GetApiKeyRequest) {
                final GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                if (this.restrictActionsToAuthenticatedUser) {
                    return checkIfUserIsOwnerOfApiKeys(authentication, getApiKeyRequest.getApiKeyId(), getApiKeyRequest.getUserName(),
                            getApiKeyRequest.getRealmName());
                } else {
                    return true;
                }
            } else if (request instanceof InvalidateApiKeyRequest) {
                final InvalidateApiKeyRequest invalidateApiKeyRequest = (InvalidateApiKeyRequest) request;
                if (this.restrictActionsToAuthenticatedUser) {
                    return checkIfUserIsOwnerOfApiKeys(authentication, invalidateApiKeyRequest.getId(),
                            invalidateApiKeyRequest.getUserName(), invalidateApiKeyRequest.getRealmName());
                } else {
                    return true;
                }
            }
            return false;
        };
    }

    private boolean checkIfUserIsOwnerOfApiKeys(Authentication authentication, String apiKeyId, String username, String realmName) {
        if (authentication.getAuthenticatedBy().getType().equals("_es_api_key")) {
            // API key id from authentication must match the id from request
            String authenticatedApiKeyId = (String) authentication.getMetadata().get("_security_api_key_id");
            if (Strings.hasText(apiKeyId)) {
                return apiKeyId.equals(authenticatedApiKeyId);
            }
        } else {
            String authenticatedUserPrincipal = authentication.getUser().principal();
            String authenticatedUserRealm = authentication.getAuthenticatedBy().getName();
            if (Strings.hasText(username) && Strings.hasText(realmName)) {
                return username.equals(authenticatedUserPrincipal) && realmName.equals(authenticatedUserRealm);
            }
        }
        return false;
    }

    @Override
    public ClusterPrivilege getPrivilege() {
        return privilege;
    }

    @Override
    public BiPredicate<TransportRequest, Authentication> getRequestPredicate() {
        return requestPredicate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(privilege, restrictActionsToAuthenticatedUser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ManageApiKeyConditionalClusterPrivilege that = (ManageApiKeyConditionalClusterPrivilege) o;
        return Objects.equals(this.privilege, that.privilege)
                && Objects.equals(this.restrictActionsToAuthenticatedUser, that.restrictActionsToAuthenticatedUser);
    }

}
