/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Predicate;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.isIndexAction;

public interface AuthorizationDenialMessages {
    String actionDenied(
        Authentication authentication,
        @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo,
        String action,
        TransportRequest request,
        @Nullable String context
    );

    String runAsDenied(Authentication authentication, @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo, String action);

    String remoteActionDenied(
        Authentication authentication,
        @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo,
        String action,
        String clusterAlias
    );

    class Default implements AuthorizationDenialMessages {
        public Default() {}

        @Override
        public String runAsDenied(
            Authentication authentication,
            @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo,
            String action
        ) {
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

        @Override
        public String actionDenied(
            Authentication authentication,
            @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo,
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
                final Collection<String> privileges = findClusterPrivilegesThatGrant(authentication, action, request);
                if (privileges != null && false == privileges.isEmpty()) {
                    message = message
                        + ", this action is granted by the cluster privileges ["
                        + collectionToCommaDelimitedString(privileges)
                        + "]";
                }
            } else if (isIndexAction(action)) {
                // this includes `all`
                final Collection<String> privileges = findIndexPrivilegesThatGrant(
                    action,
                    p -> p.getSelectorPredicate().test(IndexComponentSelector.DATA)
                );
                // this is an invariant since `all` is included in the above so the only way
                // we can get an empty result here is a bogus action, which will never be covered by a failures privilege
                assert false == privileges.isEmpty()
                    || findIndexPrivilegesThatGrant(
                        action,
                        p -> p.getSelectorPredicate().test(IndexComponentSelector.FAILURES)
                            && false == p.getSelectorPredicate().test(IndexComponentSelector.DATA)
                    ).isEmpty()
                    : "action [" + action + "] is not covered by any regular index privilege, only by failures-selector privileges";

                if (false == privileges.isEmpty()) {
                    message = message
                        + ", this action is granted by the index privileges ["
                        + collectionToCommaDelimitedString(privileges)
                        + "]";

                    final Collection<String> privilegesForFailuresOnly = findIndexPrivilegesThatGrant(
                        action,
                        p -> p.getSelectorPredicate().test(IndexComponentSelector.FAILURES)
                            && false == p.getSelectorPredicate().test(IndexComponentSelector.DATA)
                    );
                    if (false == privilegesForFailuresOnly.isEmpty() && hasIndicesWithFailuresSelector(request)) {
                        message = message
                            + " for data access, or by ["
                            + collectionToCommaDelimitedString(privilegesForFailuresOnly)
                            + "] for access with the [::failures] selector";
                    }
                }
            }
            return message;
        }

        @Override
        public String remoteActionDenied(
            Authentication authentication,
            @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo,
            String action,
            String clusterAlias
        ) {
            String userText = successfulAuthenticationDescription(authentication, authorizationInfo);
            String remoteClusterText = remoteClusterText(clusterAlias);
            String message = isIndexAction(action)
                ? " because no remote indices privileges apply for the target cluster"
                : " because no remote cluster privileges apply for the target cluster";
            return actionIsUnauthorizedMessage(action, remoteClusterText, userText) + message;
        }

        protected Collection<String> findClusterPrivilegesThatGrant(
            Authentication authentication,
            String action,
            TransportRequest request
        ) {
            return ClusterPrivilegeResolver.findPrivilegesThatGrant(action, request, authentication);
        }

        protected Collection<String> findIndexPrivilegesThatGrant(String action, Predicate<IndexPrivilege> preCondition) {
            return IndexPrivilege.findPrivilegesThatGrant(action, preCondition);
        }

        private String remoteClusterText(@Nullable String clusterAlias) {
            return Strings.format("towards remote cluster%s ", clusterAlias == null ? "" : " [" + clusterAlias + "]");
        }

        private boolean hasIndicesWithFailuresSelector(TransportRequest request) {
            String[] indices = AuthorizationEngine.RequestInfo.indices(request);
            if (indices == null) {
                return false;
            }
            for (String index : indices) {
                if (IndexNameExpressionResolver.hasSelector(index, IndexComponentSelector.FAILURES)) {
                    return true;
                }
            }
            return false;
        }

        private String authenticatedUserDescription(Authentication authentication) {
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
                    userText = successfulAuthenticationDescription(crossClusterAccessAuthentication, null)
                        + " authenticated by "
                        + userText;
                }
            }
            return userText;
        }

        // package-private for tests
        String rolesDescription(Subject subject, @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo) {
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

        // package-private for tests
        String successfulAuthenticationDescription(
            Authentication authentication,
            @Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo
        ) {
            String userText = authenticatedUserDescription(authentication);

            if (authentication.isRunAs()) {
                userText = userText + " run as [" + authentication.getEffectiveSubject().getUser().principal() + "]";
            }

            userText += rolesDescription(authentication.getEffectiveSubject(), authorizationInfo);
            return userText;
        }

        private List<String> extractEffectiveRoleNames(@Nullable AuthorizationEngine.AuthorizationInfo authorizationInfo) {
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

        private String actionIsUnauthorizedMessage(String action, String userText) {
            return actionIsUnauthorizedMessage(action, "", userText);
        }

        private String actionIsUnauthorizedMessage(String action, String remoteClusterText, String userText) {
            return "action [" + action + "] " + remoteClusterText + "is unauthorized for " + userText;
        }
    }
}
