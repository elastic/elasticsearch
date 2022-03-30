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

    static Builder builder(String userPrincipal, Boolean isAuthenticatedWithServiceAccount, String action) {
        return new Builder(userPrincipal, isAuthenticatedWithServiceAccount, action);
    }

    String toFullMessage() {
        return toMessage(true);
    }

    private String toMessage(Boolean includeSensitiveFields) {
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
                + ", this action is granted by the index privileges ["
                + collectionToCommaDelimitedString(grantingIndexPrivileges)
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

        public Builder runAsUserPrincipal(String runAsUserPrincipal) {
            this.runAsUserPrincipal = runAsUserPrincipal;
            return this;
        }

        public Builder apiKeyId(String apiKeyId) {
            this.apiKeyId = apiKeyId;
            return this;
        }

        public Builder roles(String[] roles) {
            this.roles = roles;
            return this;
        }

        public Builder context(String context) {
            this.context = context;
            return this;
        }

        public Builder grantingClusterPrivileges(Collection<String> grantingClusterPrivileges) {
            this.grantingClusterPrivileges = grantingClusterPrivileges;
            return this;
        }

        public Builder grantingIndexPrivileges(Collection<String> grantingIndexPrivileges) {
            this.grantingIndexPrivileges = grantingIndexPrivileges;
            return this;
        }
    }
}
