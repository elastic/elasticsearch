/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;

/**
 * Named cluster privilege for managing API keys owned by the current authenticated user.
 */
public class ManageOwnApiKeyClusterPrivilege implements NamedClusterPrivilege {
    public static final ManageOwnApiKeyClusterPrivilege INSTANCE = new ManageOwnApiKeyClusterPrivilege();
    private static final String PRIVILEGE_NAME = "manage_own_api_key";

    private final ClusterPermission permission;

    private ManageOwnApiKeyClusterPrivilege() {
        permission = this.buildPermission(ClusterPermission.builder()).build();
    }

    @Override
    public String name() {
        return PRIVILEGE_NAME;
    }

    @Override
    public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
        return builder.add(this, ManageOwnClusterPermissionCheck.INSTANCE);
    }

    @Override
    public ClusterPermission permission() {
        return permission;
    }

    private static final class ManageOwnClusterPermissionCheck extends ClusterPermission.ActionBasedPermissionCheck {
        public static final ManageOwnClusterPermissionCheck INSTANCE = new ManageOwnClusterPermissionCheck();

        private ManageOwnClusterPermissionCheck() {
            super(Automatons.patterns("cluster:admin/xpack/security/api_key/*"));
        }

        @Override
        protected boolean extendedCheck(String action, TransportRequest request, Authentication authentication) {
            if (request instanceof CreateApiKeyRequest) {
                return true;
            } else if (request instanceof UpdateApiKeyRequest || request instanceof BulkUpdateApiKeyRequest) {
                // Note: we return `true` here even if the authenticated entity is an API key. API keys *cannot* update themselves,
                // however this is a business logic restriction, rather than one driven solely by privileges. We therefore enforce this
                // limitation at the transport layer, in `TransportBaseUpdateApiKeyAction`.
                // Ownership of an API key, for regular users, is enforced at the service layer.
                return true;
            } else if (request instanceof final GetApiKeyRequest getApiKeyRequest) {
                // An API key requires manage_api_key privilege or higher to view any limited-by role descriptors
                if (authentication.isApiKey() && getApiKeyRequest.withLimitedBy()) {
                    return false;
                }
                return checkIfUserIsOwnerOfApiKeys(
                    authentication,
                    getApiKeyRequest.getApiKeyId(),
                    getApiKeyRequest.getUserName(),
                    getApiKeyRequest.getRealmName(),
                    getApiKeyRequest.ownedByAuthenticatedUser()
                );
            } else if (request instanceof final InvalidateApiKeyRequest invalidateApiKeyRequest) {
                final String[] apiKeyIds = invalidateApiKeyRequest.getIds();
                if (apiKeyIds == null) {
                    return checkIfUserIsOwnerOfApiKeys(
                        authentication,
                        null,
                        invalidateApiKeyRequest.getUserName(),
                        invalidateApiKeyRequest.getRealmName(),
                        invalidateApiKeyRequest.ownedByAuthenticatedUser()
                    );
                } else {
                    return Arrays.stream(apiKeyIds)
                        .allMatch(
                            id -> checkIfUserIsOwnerOfApiKeys(
                                authentication,
                                id,
                                invalidateApiKeyRequest.getUserName(),
                                invalidateApiKeyRequest.getRealmName(),
                                invalidateApiKeyRequest.ownedByAuthenticatedUser()
                            )
                        );
                }
            } else if (request instanceof final QueryApiKeyRequest queryApiKeyRequest) {
                // An API key requires manage_api_key privilege or higher to view any limited-by role descriptors
                if (authentication.isApiKey() && queryApiKeyRequest.withLimitedBy()) {
                    return false;
                }
                return queryApiKeyRequest.isFilterForCurrentUser();
            } else if (request instanceof GrantApiKeyRequest) {
                return false;
            }
            String message = "manage own api key privilege only supports API key requests (not " + request.getClass().getName() + ")";
            assert false : message;
            throw new IllegalArgumentException(message);
        }

        @Override
        protected boolean doImplies(ClusterPermission.ActionBasedPermissionCheck permissionCheck) {
            return permissionCheck instanceof ManageOwnClusterPermissionCheck;
        }

        private static boolean checkIfUserIsOwnerOfApiKeys(
            Authentication authentication,
            String apiKeyId,
            String username,
            String realmName,
            boolean ownedByAuthenticatedUser
        ) {
            if (isCurrentAuthenticationUsingSameApiKeyIdFromRequest(authentication, apiKeyId)) {
                return true;
            } else {
                /*
                 * TODO bizybot we need to think on how we can propagate appropriate error message to the end user when username, realm name
                 *   is missing. This is similar to the problem of propagating right error messages in case of access denied.
                 */
                if (authentication.isApiKey()) {
                    // API key cannot own any other API key so deny access
                    return false;
                } else if (ownedByAuthenticatedUser) {
                    return true;
                } else if (Strings.hasText(username) && Strings.hasText(realmName)) {
                    if (false == username.equals(authentication.getEffectiveSubject().getUser().principal())) {
                        return false;
                    }
                    RealmDomain domain = authentication.getEffectiveSubject().getRealm().getDomain();
                    if (domain != null) {
                        return domain.realms().stream().anyMatch(realmIdentifier -> realmName.equals(realmIdentifier.getName()));
                    } else {
                        return realmName.equals(authentication.getEffectiveSubject().getRealm().getName());
                    }
                }
            }
            return false;
        }

        private static boolean isCurrentAuthenticationUsingSameApiKeyIdFromRequest(Authentication authentication, String apiKeyId) {
            if (authentication.isApiKey()) {
                // API key id from authentication must match the id from request
                final String authenticatedApiKeyId = (String) authentication.getAuthenticatingSubject()
                    .getMetadata()
                    .get(AuthenticationField.API_KEY_ID_KEY);
                if (Strings.hasText(apiKeyId)) {
                    return apiKeyId.equals(authenticatedApiKeyId);
                }
            }
            return false;
        }
    }
}
