/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;

import java.util.Collection;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;

record AuthorizationDenialMessage(
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

    static Builder builder(String userPrincipal, Boolean isAuthenticatedWithServiceAccount, String action) {
        return new Builder(userPrincipal, isAuthenticatedWithServiceAccount, action);
    }

    String asLogMessage() {
        return asMessage(true);
    }

    String asExceptionMessage() {
        return asMessage(false);
    }

    private String asMessage(Boolean includeSensitiveFields) {
        String userText = (isAuthenticatedWithServiceAccount ? "service account" : "user") + " [" + userPrincipal + "]";

        if (runAsUserPrincipal != null) {
            userText = userText + " run as [" + runAsUserPrincipal + "]";
        }

        if (apiKeyId != null) {
            userText = "API key id [" + apiKeyId + "] of " + userText;
        }

        if (includeSensitiveFields && roles != null) {
            userText = userText + " with roles [" + Strings.arrayToCommaDelimitedString(roles) + "]";
        }

        String message = "action [" + action + "] is unauthorized for " + userText;

        if (context != null) {
            message = message + " " + context;
        }

        if (includeSensitiveFields && grantingClusterPrivileges != null) {
            message = message
                + ", this action is granted by the cluster privileges ["
                + collectionToCommaDelimitedString(grantingClusterPrivileges)
                + "]";
        }

        if (includeSensitiveFields && grantingIndexPrivileges != null) {
            message = message
                + ", this action is granted by the cluster privileges ["
                + collectionToCommaDelimitedString(grantingClusterPrivileges)
                + "]";
        }

        return message;
    }

    static class Builder {
        private final String userPrincipal;
        private final Boolean isAuthenticatedWithServiceAccount;
        private final String action;
        private String runAsUserPrincipal;
        private String apiKeyId;
        private String[] roles;
        private String context;
        private Collection<String> grantingClusterPrivileges;
        private Collection<String> grantingIndexPrivileges;

        Builder(String userPrincipal, Boolean isAuthenticatedWithServiceAccount, String action) {
            this.userPrincipal = userPrincipal;
            this.action = action;
            this.isAuthenticatedWithServiceAccount = isAuthenticatedWithServiceAccount;
        }

        AuthorizationDenialMessage createAuthorizationDenialMessage() {
            return new AuthorizationDenialMessage(
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

        public Builder setRunAsUserPrincipal(String runAsUserPrincipal) {
            this.runAsUserPrincipal = runAsUserPrincipal;
            return this;
        }

        public Builder setApiKeyId(String apiKeyId) {
            assert apiKeyId != null : "api key id must be present in the metadata";
            this.apiKeyId = apiKeyId;
            return this;
        }

        public Builder setRoles(String[] roles) {
            this.roles = roles;
            return this;
        }

        public Builder setContext(String context) {
            this.context = context;
            return this;
        }

        public Builder setGrantingClusterPrivileges(Collection<String> grantingClusterPrivileges) {
            this.grantingClusterPrivileges = grantingClusterPrivileges;
            return this;
        }

        public Builder setGrantingIndexPrivileges(Collection<String> grantingIndexPrivileges) {
            this.grantingIndexPrivileges = grantingIndexPrivileges;
            return this;
        }
    }
}
