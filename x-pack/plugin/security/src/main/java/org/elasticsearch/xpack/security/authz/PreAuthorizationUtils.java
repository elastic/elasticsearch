/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.ParentActionAuthorization;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class PreAuthorizationUtils {

    private static final Logger logger = LogManager.getLogger(PreAuthorizationUtils.class);

    /**
     * This map holds parent-child action relationships for which we can optimize authorization
     * and skip authorization for child actions if the parent action is successfully authorized.
     * Normally every action would be authorized on a local node on which it's being executed.
     * Here we define all child actions for which the authorization can be safely skipped
     * on a remote node as they only access a subset of resources.
     */
    public static final Map<String, Set<String>> CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT = Map.of(
        SearchAction.NAME,
        Set.of(
            SearchTransportService.FREE_CONTEXT_ACTION_NAME,
            SearchTransportService.DFS_ACTION_NAME,
            SearchTransportService.QUERY_ACTION_NAME,
            SearchTransportService.QUERY_ID_ACTION_NAME,
            SearchTransportService.FETCH_ID_ACTION_NAME,
            SearchTransportService.QUERY_CAN_MATCH_NAME,
            SearchTransportService.QUERY_CAN_MATCH_NODE_NAME
        )
    );

    /**
     * This method sets {@link ParentActionAuthorization} as a header in the thread context,
     * which will be used for skipping authorization of child actions if the following conditions are met:
     *
     * <ul>
     * <li>parent action is one of the white listed in {@link #CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT}</li>
     * <li>FLS and DLS are not configured</li>
     * <li>RBACEngine was used to authorize parent request and not a custom authorization engine</li>
     * </ul>
     */
    public static void maybeSkipChildrenActionAuthorization(
        SecurityContext securityContext,
        AuthorizationContext parentAuthorizationContext
    ) {
        final String parentAction = parentAuthorizationContext.getAction();
        if (CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT.containsKey(parentAction) == false) {
            // This is not one of the white listed parent actions.
            return;
        }

        final IndicesAccessControl indicesAccessControl = parentAuthorizationContext.getIndicesAccessControl();
        if (indicesAccessControl == null) {
            // This can happen if the parent request was authorized by index name only - e.g. bulk request
            // A missing IAC is not an error, but it means we can't safely tie authz of the child action to the parent authz
            return;
        }

        // Just a sanity check. If we ended up here, the authz should have been granted.
        if (indicesAccessControl.isGranted() == false) {
            return;
        }

        final Role role = RBACEngine.maybeGetRBACEngineRole(parentAuthorizationContext.getAuthorizationInfo());
        if (role == null) {
            // If role is null, it means a custom authorization engine is in use, hence we cannot do the optimization here.
            return;
        }

        // We can't safely pre-authorize actions if DLS or FLS is configured without passing IAC as well with the authorization result.
        // For simplicity, we only pre-authorize actions when FLS and DLS are not configured, otherwise we would have to compute and send
        // indices access control as well, which we want to avoid with this optimization.
        if (role.hasFieldOrDocumentLevelSecurity()) {
            return;
        }

        final ParentActionAuthorization existingParentAuthorization = securityContext.getParentAuthorization();
        if (existingParentAuthorization != null) {
            throw new AssertionError(
                "found parent authorization for action ["
                    + existingParentAuthorization.action()
                    + "] while attempting to set authorization for new parent action ["
                    + parentAction
                    + "]"
            );
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("adding authorization for parent action [" + parentAction + "] to the thread context");
            }
            securityContext.setParentAuthorization(new ParentActionAuthorization(parentAction));
        }
    }

    private static boolean shouldPreAuthorizeChildActionOfParent(final String parent, final String child) {
        final Set<String> children = CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT.get(parent);
        return children != null && children.contains(child);
    }

    public static boolean shouldRemoveParentAuthorizationFromThreadContext(
        Optional<String> remoteClusterAlias,
        String childAction,
        SecurityContext securityContext
    ) {
        final ParentActionAuthorization parentAuthorization = securityContext.getParentAuthorization();
        if (parentAuthorization == null) {
            // Nothing to remove.
            return false;
        }

        if (remoteClusterAlias.isPresent()) {
            // We never want to send the parent authorization header to remote clusters.
            return true;
        }

        if (shouldPreAuthorizeChildActionOfParent(parentAuthorization.action(), childAction) == false) {
            // We want to remove the parent authorization header if the child action is not one of the white listed.
            return true;
        }

        return false;
    }

    public static boolean shouldPreAuthorizeChildByParentAction(RequestInfo childRequestInfo, AuthorizationInfo childAuthorizationInfo) {

        final ParentActionAuthorization parentAuthorization = childRequestInfo.getParentAuthorization();
        if (parentAuthorization == null) {
            return false;
        }

        Role role = RBACEngine.maybeGetRBACEngineRole(childAuthorizationInfo);
        if (role == null) {
            // If role is null, it means a custom authorization engine is in use.
            return false;
        }
        if (role.hasFieldOrDocumentLevelSecurity()) {
            // We can't safely pre-authorize actions if DLS or FLS is configured
            // without sending IAC as well with authorization result.
            return false;
        }

        final String parentAction = parentAuthorization.action();
        final String childAction = childRequestInfo.getAction();
        if (shouldPreAuthorizeChildActionOfParent(parentAction, childAction) == false) {
            // We only pre-authorize explicitly allowed child actions.
            return false;
        }

        final IndicesRequest indicesRequest;
        if (childRequestInfo.getRequest() instanceof IndicesRequest) {
            indicesRequest = (IndicesRequest) childRequestInfo.getRequest();
        } else {
            // Can only handle indices request here
            return false;
        }

        final String[] indices = indicesRequest.indices();
        if (indices == null || indices.length == 0) {
            // No indices to check
            return false;
        }

        if (Arrays.equals(IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY, indices)) {
            // Special placeholder for no indices.
            // We probably can short circuit this, but it's safer not to and just fall through to the regular authorization
            return false;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("pre-authorizing child action [" + childAction + "] of parent action [" + parentAction + "]");
        }
        return true;
    }

    private PreAuthorizationUtils() {
        throw new IllegalAccessError();
    }
}
