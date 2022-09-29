/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.ParentIndexActionAuthorization;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SecurityProfileUser;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.ingest.IngestService.INGEST_ORIGIN;
import static org.elasticsearch.persistent.PersistentTasksService.PERSISTENT_TASK_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.IDP_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ROLLUP_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.STACK_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;

public final class AuthorizationUtils {

    private static final Logger logger = LogManager.getLogger(AuthorizationUtils.class);

    private static final Predicate<String> INTERNAL_PREDICATE = Automatons.predicate("internal:*");

    private AuthorizationUtils() {}

    /**
     * This method is used to determine if a request should be executed as the system user, even if the request already
     * has a user associated with it.
     *
     * In order for the user to be replaced by the system user one of the following conditions must be true:
     *
     * <ul>
     *     <li>the action is an internal action and no user is associated with the request</li>
     *     <li>the action is an internal action and the thread context contains a non-internal action as the originating action</li>
     * </ul>
     *
     * @param threadContext the {@link ThreadContext} that contains the headers and context associated with the request
     * @param action the action name that is being executed
     * @return true if the system user should be used to execute a request
     */
    public static boolean shouldReplaceUserWithSystem(ThreadContext threadContext, String action) {
        // the action must be internal OR the thread context must be a system context.
        if (threadContext.isSystemContext() == false && isInternalAction(action) == false) {
            return false;
        }

        // there is no authentication object AND we are executing in a system context OR an internal action
        // AND there
        Authentication authentication = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        if (authentication == null && threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME) == null) {
            return true;
        }

        // we have a internal action being executed by a user other than the system user, lets verify that there is a
        // originating action that is not a internal action. We verify that there must be a originating action as an
        // internal action should never be called by user code from a client
        final String originatingAction = threadContext.getTransient(AuthorizationServiceField.ORIGINATING_ACTION_KEY);
        if (originatingAction != null && isInternalAction(originatingAction) == false) {
            return true;
        }

        // either there was no originating action or the originating action was an internal action,
        // we should not replace under these circumstances
        return false;
    }

    /**
     * Returns true if the thread context contains the origin of the action and does not have any authentication
     */
    public static boolean shouldSetUserBasedOnActionOrigin(ThreadContext context) {
        final String actionOrigin = context.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME);
        final Authentication authentication = context.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        return actionOrigin != null && authentication == null;
    }

    /**
     * Stashes the current context and executes the consumer as the proper user based on the origin of the action.
     *
     * This method knows nothing about listeners so it is important that callers ensure their listeners preserve their
     * context and restore it appropriately.
     */
    public static void switchUserBasedOnActionOriginAndExecute(
        ThreadContext threadContext,
        SecurityContext securityContext,
        Version version,
        Consumer<ThreadContext.StoredContext> consumer
    ) {
        final String actionOrigin = threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME);
        if (actionOrigin == null) {
            assert false : "cannot switch user if there is no action origin";
            throw new IllegalStateException("cannot switch user if there is no action origin");
        }

        switch (actionOrigin) {
            case SECURITY_ORIGIN:
                securityContext.executeAsInternalUser(XPackSecurityUser.INSTANCE, version, consumer);
                break;
            case SECURITY_PROFILE_ORIGIN:
                securityContext.executeAsInternalUser(SecurityProfileUser.INSTANCE, version, consumer);
                break;
            case WATCHER_ORIGIN:
            case ML_ORIGIN:
            case MONITORING_ORIGIN:
            case TRANSFORM_ORIGIN:
            case DEPRECATION_ORIGIN:
            case PERSISTENT_TASK_ORIGIN:
            case ROLLUP_ORIGIN:
            case INDEX_LIFECYCLE_ORIGIN:
            case ENRICH_ORIGIN:
            case IDP_ORIGIN:
            case INGEST_ORIGIN:
            case STACK_ORIGIN:
            case SEARCHABLE_SNAPSHOTS_ORIGIN:
            case LOGSTASH_MANAGEMENT_ORIGIN:
            case FLEET_ORIGIN:
            case TASKS_ORIGIN:   // TODO use a more limited user for tasks
                securityContext.executeAsInternalUser(XPackUser.INSTANCE, version, consumer);
                break;
            case ASYNC_SEARCH_ORIGIN:
                securityContext.executeAsInternalUser(AsyncSearchUser.INSTANCE, version, consumer);
                break;
            default:
                assert false : "action.origin [" + actionOrigin + "] is unknown!";
                throw new IllegalStateException("action.origin [" + actionOrigin + "] should always be a known value");
        }
    }

    public static void maybePreAuthorizeChildAction(
        ThreadContext threadContext,
        ClusterState clusterState,
        String childAction,
        DiscoveryNode destinationNode,
        TransportRequest request
    ) {

        AuthorizationEngine.AuthorizationContext parentContext = extractAuthorizationContext(threadContext, childAction);
        if (parentContext == null) {
            return;
        }

        final Role role = RBACEngine.maybeGetRBACEngineRole(parentContext.getAuthorizationInfo());
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

        if (childAction.startsWith(parentContext.getAction()) == false) {
            // Parent action is not a true parent
            // We want to treat shard level actions (those that append '[s]' and/or '[p]' & '[r]')
            // or similar (e.g. search phases) as children, but not every action that is triggered
            // within another action should be authorized this way
            return;
        }

        final IndicesRequest indicesRequest;
        if (request instanceof IndicesRequest) {
            indicesRequest = (IndicesRequest) request;
        } else {
            // We can only pre-authorize indices request.
            return;
        }

        final String[] indices = indicesRequest.indices();
        if (indices == null || indices.length == 0 || Arrays.equals(IndicesAndAliasesResolver.NO_INDICES_OR_ALIASES_ARRAY, indices)) {
            // No indices to check
            return;
        }

        if (clusterState.nodes().getLocalNode().equals(destinationNode)) {
            // Child actions targeting local (same) node are handled differently and already pre-authorized by parent action.
            return;
        }

        if (clusterState.nodes().nodeExists(destinationNode) == false) {
            // We can only pre-authorize actions targeting node which belongs to the same cluster.
            return;
        }

        try {
            Optional<ParentIndexActionAuthorization> existingParentAuthorization = Optional.ofNullable(
                ParentIndexActionAuthorization.readFromThreadContext(threadContext)
            );
            if (existingParentAuthorization.isPresent()) {
                if (existingParentAuthorization.get().action().equals(parentContext.getAction())) {
                    // Single request can fan-out a child action to multiple nodes in the cluster, e.g. node1 and node2.
                    // Sending a child action to node1 would have already put parent authorization in the thread context.
                    // To avoid attempting to pre-authorize the same parent action twice we simply return here
                    // since pre-authorization is already set in the context.
                    return;
                } else {
                    throw new AssertionError(
                        "Pre-authorization of parent action ["
                            + existingParentAuthorization.get().action()
                            + "] found while attempting to pre-authorize child action ["
                            + childAction
                            + "] of parent ["
                            + parentContext.getAction()
                            + "]"
                    );
                }
            } else {
                logger.debug("Pre-authorizing child action [" + childAction + "] of parent action [" + parentContext.getAction() + "]");
                new ParentIndexActionAuthorization(
                    Version.CURRENT,
                    parentContext.getAction(),
                    parentContext.getIndicesAccessControl().isGranted()
                ).writeToThreadContext(threadContext);
            }
        } catch (Exception e) {
            logger.error("Failed to write authorization to thread context.", e);
            throw internalError(
                "Failed to write pre-authorization for child action ["
                    + childAction
                    + "] of parent action ["
                    + parentContext.getAction()
                    + "] to the context"
            );
        }
    }

    @Nullable
    public static AuthorizationEngine.AuthorizationContext extractAuthorizationContext(ThreadContext threadContext, String childAction) {
        final String originatingAction = threadContext.getTransient(ORIGINATING_ACTION_KEY);
        if (Strings.isNullOrEmpty(originatingAction)) {
            // No parent action
            return null;
        }
        AuthorizationEngine.AuthorizationInfo authorizationInfo = threadContext.getTransient(AUTHORIZATION_INFO_KEY);
        if (authorizationInfo == null) {
            throw internalError(
                "While attempting to authorize action ["
                    + childAction
                    + "], found originating action ["
                    + originatingAction
                    + "] but no authorization info"
            );
        }

        final IndicesAccessControl parentAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        return new AuthorizationEngine.AuthorizationContext(originatingAction, authorizationInfo, parentAccessControl);
    }

    private static ElasticsearchSecurityException internalError(String message) {
        // When running with assertions enabled (testing) kill the node so that there is a hard failure in CI
        assert false : message;
        // Otherwise (production) just throw an exception so that we don't authorize something incorrectly
        return new ElasticsearchSecurityException(message);
    }

    private static boolean isInternalAction(String action) {
        return INTERNAL_PREDICATE.test(action);
    }
}
