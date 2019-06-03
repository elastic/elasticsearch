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
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Conditional cluster privilege for managing API keys
 */
public final class ManageApiKeyConditionalClusterPrivilege implements PlainConditionalClusterPrivilege {

    private static final String CREATE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/create";
    private static final String GET_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/get";
    private static final String INVALIDATE_API_KEY_PATTERN = "cluster:admin/xpack/security/api_key/invalidate";
    private static final List<String> API_KEY_ACTION_PATTERNS = List.of(CREATE_API_KEY_PATTERN, GET_API_KEY_PATTERN,
            INVALIDATE_API_KEY_PATTERN);

    private final Set<String> realms;
    private final Predicate<String> realmsPredicate;
    private final Set<String> users;
    private final Predicate<String> usersPredicate;
    private final ClusterPrivilege privilege;
    private final BiPredicate<TransportRequest, Authentication> requestPredicate;

    public ManageApiKeyConditionalClusterPrivilege(Set<String> actions, Set<String> realms, Set<String> users) {
        // validate allowed actions
        for (String action : actions) {
            if (ClusterPrivilege.MANAGE_API_KEY.predicate().test(action) == false) {
                throw new IllegalArgumentException("invalid action [ " + action + " ] specified, expected API key privilege actions [ "
                        + API_KEY_ACTION_PATTERNS + " ]");
            }
        }
        this.privilege = ClusterPrivilege.get(actions).v1();

        this.realms = (realms == null) ? Collections.emptySet() : Set.copyOf(realms);
        this.realmsPredicate = Automatons.predicate(this.realms);
        this.users = (users == null) ? Collections.emptySet() : Set.copyOf(users);
        this.usersPredicate = Automatons.predicate(this.users);

        if (this.realms.contains("_self") && this.users.contains("_self") == false
                || this.users.contains("_self") && this.realms.contains("_self") == false) {
            throw new IllegalArgumentException(
                    "both realms and users must contain only `_self` when restricting access of API keys to owner");
        }
        if (this.realms.contains("_self") && this.users.contains("_self")) {
            if (this.realms.size() > 1 || this.users.size() > 1) {
                throw new IllegalArgumentException(
                        "both realms and users must contain only `_self` when restricting access of API keys to owner");
            }
        }

        this.requestPredicate = (request, authentication) -> {
            if (request instanceof CreateApiKeyRequest) {
                return true;
            } else if (request instanceof GetApiKeyRequest) {
                final GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                if (this.realms.contains("_self") && this.users.contains("_self")) {
                    return checkIfUserIsOwnerOfApiKeys(authentication, getApiKeyRequest.getApiKeyId(), getApiKeyRequest.getUserName(),
                            getApiKeyRequest.getRealmName());
                } else {
                    return checkIfAccessAllowed(realms, getApiKeyRequest.getRealmName(), realmsPredicate)
                            && checkIfAccessAllowed(users, getApiKeyRequest.getUserName(), usersPredicate);
                }
            } else if (request instanceof InvalidateApiKeyRequest) {
                final InvalidateApiKeyRequest invalidateApiKeyRequest = (InvalidateApiKeyRequest) request;
                if (this.realms.contains("_self") && this.users.contains("_self")) {
                    return checkIfUserIsOwnerOfApiKeys(authentication, invalidateApiKeyRequest.getId(),
                            invalidateApiKeyRequest.getUserName(), invalidateApiKeyRequest.getRealmName());
                } else {
                    return checkIfAccessAllowed(realms, invalidateApiKeyRequest.getRealmName(), realmsPredicate)
                            && checkIfAccessAllowed(users, invalidateApiKeyRequest.getUserName(), usersPredicate);
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

    private static boolean checkIfAccessAllowed(Set<String> names, String requestName, Predicate<String> predicate) {
        return (Strings.hasText(requestName) == false) ? names.contains("*") : predicate.test(requestName);
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
        return Objects.hash(privilege, users, realms);
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
        return Objects.equals(this.privilege, that.privilege) && Objects.equals(this.realms, that.realms)
                && Objects.equals(this.users, that.users);
    }

}
