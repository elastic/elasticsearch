/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.isIndexAction;

class AuthorizationDenialMessages {

    private AuthorizationDenialMessages() {}

    static String runAsDenied(Authentication authentication, @Nullable AuthorizationInfo authorizationInfo, String action) {
        assert authentication.isRunAs() : "constructing run as denied message but authentication for action was not run as";

        String userText = authenticatedUserDescription(authentication);
        String actionIsUnauthorizedMessage = actionIsUnauthorizedMessage(action, userText);

        String unauthorizedToRunAsMessage = "because "
            + userText
            + " is unauthorized to run as ["
            + authentication.getUser().principal()
            + "]";

        return actionIsUnauthorizedMessage + rolesDescription(authentication, authorizationInfo) + ", " + unauthorizedToRunAsMessage;
    }

    static String actionDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action,
        TransportRequest request,
        @Nullable String context
    ) {
        String userText = authenticatedUserDescription(authentication);

        if (authentication.isRunAs()) {
            userText = userText + " run as [" + authentication.getUser().principal() + "]";
        }

        userText += rolesDescription(authentication, authorizationInfo);

        String message = actionIsUnauthorizedMessage(action, userText);
        if (context != null) {
            message = message + " " + context;
        }

        if (ClusterPrivilegeResolver.isClusterAction(action)) {
            final Collection<String> privileges = ClusterPrivilegeResolver.findPrivilegesThatGrant(action, request, authentication);
            if (privileges != null && privileges.size() > 0) {
                message = message
                    + ", this action is granted by the cluster privileges ["
                    + collectionToCommaDelimitedString(privileges)
                    + "]";
            }
        } else if (isIndexAction(action)) {
            final Collection<String> privileges = IndexPrivilege.findPrivilegesThatGrant(action);
            if (privileges != null && privileges.size() > 0) {
                message = message
                    + ", this action is granted by the index privileges ["
                    + collectionToCommaDelimitedString(privileges)
                    + "]";
            }
        }

        return message;
    }

    private static String authenticatedUserDescription(Authentication authentication) {
        String userText = (authentication.isAuthenticatedWithServiceAccount() ? "service account" : "user")
            + " ["
            + authentication.getAuthenticatingSubject().getUser().principal()
            + "]";
        if (authentication.isAuthenticatedAsApiKey()) {
            final String apiKeyId = (String) authentication.getMetadata().get(AuthenticationField.API_KEY_ID_KEY);
            assert apiKeyId != null : "api key id must be present in the metadata";
            userText = "API key id [" + apiKeyId + "] of " + userText;
        }
        return userText;
    }

    private static String rolesDescription(Authentication authentication, @Nullable AuthorizationInfo authorizationInfo) {
        // The run-as user is always from a realm. So it must have roles that can be printed.
        // If the user is not run-as, we cannot print the roles if it's an API key or a service account (both do not have
        // roles, but privileges)
        if (false == authentication.isServiceAccount() && false == authentication.isApiKey()) {
            final StringBuilder sb = new StringBuilder();
            final boolean hasDeclaredRoleNames = authentication.getEffectiveSubject().getUser().roles().length > 0;
            if (hasDeclaredRoleNames) {
                sb.append(" with declared roles [")
                    .append(Strings.arrayToCommaDelimitedString(authentication.getEffectiveSubject().getUser().roles()))
                    .append("]");
            } else {
                sb.append(" with no declared roles");
            }
            if (authorizationInfo == null) {
                return sb.toString();
            }

            final Set<String> resolvedRoleNames;
            final Role role = RBACEngine.maybeGetRBACEngineRole(authorizationInfo);
            if (role == Role.EMPTY) {
                resolvedRoleNames = Set.of();
            } else {
                resolvedRoleNames = Set.of((String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
            }
            if (hasDeclaredRoleNames) {
                sb.append(" (");
                final Set<String> declaredRoleNames = Set.of(authentication.getEffectiveSubject().getUser().roles());
                final Set<String> intersection = Sets.intersection(declaredRoleNames, resolvedRoleNames);
                if (intersection.equals(declaredRoleNames)) {
                    sb.append("all resolved");
                } else {
                    if (intersection.isEmpty()) {
                        sb.append("none resolved");
                    } else {
                        sb.append("unresolved [")
                            .append(Strings.collectionToCommaDelimitedString(Sets.sortedDifference(declaredRoleNames, intersection)))
                            .append("]");
                    }
                }
                final SortedSet<String> additionalRoleNames = Sets.sortedDifference(resolvedRoleNames, intersection);
                if (false == additionalRoleNames.isEmpty()) {
                    sb.append(", additionally resolved [")
                        .append(Strings.collectionToCommaDelimitedString(additionalRoleNames))
                        .append("]");
                }
                sb.append(")");
            } else {
                if (false == resolvedRoleNames.isEmpty()) {
                    sb.append("(additionally resolved [")
                        .append(Strings.collectionToCommaDelimitedString(resolvedRoleNames.stream().sorted().toList()));
                    sb.append("])");
                }
            }
            return sb.toString();
        } else {
            return "";
        }
    }

    private static String actionIsUnauthorizedMessage(String action, String userText) {
        return "action [" + action + "] is unauthorized for " + userText;
    }
}
