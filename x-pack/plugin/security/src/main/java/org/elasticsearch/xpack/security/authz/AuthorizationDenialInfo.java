/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collection;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.isIndexAction;

record AuthorizationDenialInfo(
    String userPrincipal,
    Boolean isAuthenticatedWithServiceAccount,
    String action,
    String runAsUserPrincipal,
    String apiKeyId,
    String[] roles,
    String context,
    Collection<String> grantingClusterPrivileges,
    Collection<String> grantingIndexPrivileges
) {

    static Builder builder(Authentication authentication, String action) {
        return new Builder(authentication, action);
    }

    String toMessage() {
        String userText = (isAuthenticatedWithServiceAccount ? "service account" : "user") + " [" + userPrincipal + "]";

        if (runAsUserPrincipal != null) {
            userText = userText + " run as [" + runAsUserPrincipal + "]";
        }

        if (apiKeyId != null) {
            userText = "API key id [" + apiKeyId + "] of " + userText;
        }

        if (roles != null) {
            userText = userText + " with roles [" + Strings.arrayToCommaDelimitedString(roles) + "]";
        }

        String message = "action [" + action + "] is unauthorized for " + userText;

        if (context != null) {
            message = message + " " + context;
        }

        if (grantingClusterPrivileges != null) {
            message = message
                + ", this action is granted by the cluster privileges ["
                + collectionToCommaDelimitedString(grantingClusterPrivileges)
                + "]";
        }

        if (grantingIndexPrivileges != null) {
            message = message
                + ", this action is granted by the index privileges ["
                + collectionToCommaDelimitedString(grantingIndexPrivileges)
                + "]";
        }

        return message;
    }

    static class Builder {
        private final Authentication authentication;
        private final String userPrincipal;
        private final Boolean isAuthenticatedWithServiceAccount;
        private final String action;
        private String runAsUserPrincipal;
        private String apiKeyId;
        private String[] roles;
        private String context;
        private Collection<String> grantingClusterPrivileges;
        private Collection<String> grantingIndexPrivileges;

        Builder(Authentication authentication, String action) {
            this.authentication = authentication;
            this.action = action;
            this.userPrincipal = authentication.getUser().authenticatedUser().principal();
            this.isAuthenticatedWithServiceAccount = authentication.isAuthenticatedWithServiceAccount();
        }

        AuthorizationDenialInfo build() {
            return new AuthorizationDenialInfo(
                userPrincipal,
                isAuthenticatedWithServiceAccount,
                action,
                runAsUserPrincipal,
                apiKeyId,
                roles,
                context,
                grantingClusterPrivileges,
                grantingIndexPrivileges
            );
        }

        public Builder withRunAsUserPrincipal() {
            User user = authentication.getUser();
            if (user.isRunAs()) {
                this.runAsUserPrincipal = user.principal();
            }
            return this;
        }

        public Builder withApiKeyId() {
            if (authentication.isAuthenticatedAsApiKey()) {
                String apiKeyId = (String) authentication.getMetadata().get(AuthenticationField.API_KEY_ID_KEY);
                assert apiKeyId != null : "api key id must be present in the metadata";
                this.apiKeyId = apiKeyId;
            }
            return this;
        }

        public Builder withRoles() {
            // The run-as user is always from a realm. So it must have roles that can be printed.
            // If the user is not run-as, we cannot print the roles if it's an API key or a service account (both do not have
            // roles, but privileges)
            if (false == authentication.isServiceAccount() && false == authentication.isApiKey()) {
                roles = authentication.getUser().roles();
            }
            return this;
        }

        public Builder withGrantingPrivilegeInfo(TransportRequest request) {
            if (ClusterPrivilegeResolver.isClusterAction(action)) {
                Collection<String> privileges = ClusterPrivilegeResolver.findPrivilegesThatGrant(action, request, authentication);
                if (privileges != null && privileges.size() > 0) {
                    grantingClusterPrivileges = privileges;
                }
            } else if (isIndexAction(action)) {
                Collection<String> privileges = IndexPrivilege.findPrivilegesThatGrant(action);
                if (privileges != null && privileges.size() > 0) {
                    grantingIndexPrivileges = privileges;
                }
            }
            return this;
        }

        public Builder withContext(String context) {
            this.context = context;
            return this;
        }
    }
}
