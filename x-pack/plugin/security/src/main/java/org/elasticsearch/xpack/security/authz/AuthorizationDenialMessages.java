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
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
            + authentication.getEffectiveSubject().getUser().principal()
            + "]";

        return actionIsUnauthorizedMessage
            + rolesDescription(authentication.getAuthenticatingSubject(), authorizationInfo.getAuthenticatedUserAuthorizationInfo())
            + ", "
            + unauthorizedToRunAsMessage;
    }

    static String actionDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action,
        TransportRequest request,
        @Nullable String context
    ) {
        String userText = successfulAuthenticationDescription(authentication, authorizationInfo);
        String remoteClusterText = authentication.isCrossClusterAccess() ? remoteClusterText(null) : "";
        String message = actionIsUnauthorizedMessage(action, remoteClusterText, userText);
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

    static String remoteActionDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action,
        String clusterAlias
    ) {
        assert isIndexAction(action);
        String userText = successfulAuthenticationDescription(authentication, authorizationInfo);
        String remoteClusterText = remoteClusterText(clusterAlias);
        return actionIsUnauthorizedMessage(action, remoteClusterText, userText)
            + " because no remote indices privileges apply for the target cluster";
    }

    private static String remoteClusterText(@Nullable String clusterAlias) {
        return Strings.format("towards remote cluster%s ", clusterAlias == null ? "" : " [" + clusterAlias + "]");
    }

    private static String authenticatedUserDescription(Authentication authentication) {
        String userText = (authentication.isServiceAccount() ? "service account" : "user")
            + " ["
            + authentication.getAuthenticatingSubject().getUser().principal()
            + "]";
        if (authentication.isAuthenticatedAsApiKey() || authentication.isCrossClusterAccess()) {
            final String apiKeyId = (String) authentication.getAuthenticatingSubject()
                .getMetadata()
                .get(AuthenticationField.API_KEY_ID_KEY);
            assert apiKeyId != null : "api key id must be present in the metadata";
            userText = "API key id [" + apiKeyId + "] of " + userText;
            if (authentication.isCrossClusterAccess()) {
                final Authentication crossClusterAccessAuthentication = (Authentication) authentication.getAuthenticatingSubject()
                    .getMetadata()
                    .get(AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY);
                assert crossClusterAccessAuthentication != null : "cross cluster access authentication must be present in the metadata";
                userText = successfulAuthenticationDescription(crossClusterAccessAuthentication, null) + " authenticated by " + userText;
            }
        }
        return userText;
    }

    static String rolesDescription(Subject subject, @Nullable AuthorizationInfo authorizationInfo) {
        // We cannot print the roles if it's an API key or a service account (both do not have roles, but privileges)
        if (subject.getType() != Subject.Type.USER) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        final List<String> effectiveRoleNames = extractEffectiveRoleNames(authorizationInfo);
        if (effectiveRoleNames == null) {
            sb.append(" with assigned roles [").append(Strings.arrayToCommaDelimitedString(subject.getUser().roles())).append("]");
        } else {
            sb.append(" with effective roles [").append(Strings.collectionToCommaDelimitedString(effectiveRoleNames)).append("]");

            final Set<String> assignedRoleNames = Set.of(subject.getUser().roles());
            final SortedSet<String> unfoundedRoleNames = Sets.sortedDifference(assignedRoleNames, Set.copyOf(effectiveRoleNames));
            if (false == unfoundedRoleNames.isEmpty()) {
                sb.append(" (assigned roles [")
                    .append(Strings.collectionToCommaDelimitedString(unfoundedRoleNames))
                    .append("] were not found)");
            }
        }
        return sb.toString();
    }

    static String successfulAuthenticationDescription(Authentication authentication, @Nullable AuthorizationInfo authorizationInfo) {
        String userText = authenticatedUserDescription(authentication);

        if (authentication.isRunAs()) {
            userText = userText + " run as [" + authentication.getEffectiveSubject().getUser().principal() + "]";
        }

        userText += rolesDescription(authentication.getEffectiveSubject(), authorizationInfo);
        return userText;
    }

    private static List<String> extractEffectiveRoleNames(@Nullable AuthorizationInfo authorizationInfo) {
        if (authorizationInfo == null) {
            return null;
        }

        final Map<String, Object> info = authorizationInfo.asMap();
        final Object roleNames = info.get(PRINCIPAL_ROLES_FIELD_NAME);
        // AuthorizationInfo from custom authorization engine may not have this field or have it as a different data type
        if (false == roleNames instanceof String[]) {
            assert false == authorizationInfo instanceof RBACEngine.RBACAuthorizationInfo
                : "unexpected user.roles field [" + roleNames + "] for RBACAuthorizationInfo";
            return null;
        }
        return Arrays.stream((String[]) roleNames).sorted().toList();
    }

    private static String actionIsUnauthorizedMessage(String action, String userText) {
        return actionIsUnauthorizedMessage(action, "", userText);
    }

    private static String actionIsUnauthorizedMessage(String action, String remoteClusterText, String userText) {
        return "action [" + action + "] " + remoteClusterText + "is unauthorized for " + userText;
    }
}
