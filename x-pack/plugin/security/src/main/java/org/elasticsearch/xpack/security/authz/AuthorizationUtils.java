/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.InternalUsers;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.action.bulk.TransportBulkAction.LAZY_ROLLOVER_ORIGIN;
import static org.elasticsearch.action.support.replication.PostWriteRefresh.POST_WRITE_REFRESH_ORIGIN;
import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DATA_STREAM_LIFECYCLE_ORIGIN;
import static org.elasticsearch.ingest.IngestService.INGEST_ORIGIN;
import static org.elasticsearch.persistent.PersistentTasksService.PERSISTENT_TASK_ORIGIN;
import static org.elasticsearch.synonyms.SynonymsManagementAPIService.SYNONYMS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.APM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.IDP_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.OTEL_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.PROFILING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.REINDEX_DATA_STREAM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ROLLUP_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.STACK_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public final class AuthorizationUtils {

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
        TransportVersion version,
        Consumer<ThreadContext.StoredContext> consumer
    ) {
        final String actionOrigin = threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME);
        if (actionOrigin == null) {
            assert false : "cannot switch user if there is no action origin";
            throw new IllegalStateException("cannot switch user if there is no action origin");
        }

        switch (actionOrigin) {
            case SECURITY_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.XPACK_SECURITY_USER, version, consumer);
                break;
            case SECURITY_PROFILE_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.SECURITY_PROFILE_USER, version, consumer);
                break;
            case POST_WRITE_REFRESH_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.STORAGE_USER, version, consumer);
                break;
            case DATA_STREAM_LIFECYCLE_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.DATA_STREAM_LIFECYCLE_USER, version, consumer);
                break;
            case REINDEX_DATA_STREAM_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.REINDEX_DATA_STREAM_USER, version, consumer);
                break;
            case LAZY_ROLLOVER_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.LAZY_ROLLOVER_USER, version, consumer);
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
            case PROFILING_ORIGIN:
            case APM_ORIGIN:
            case OTEL_ORIGIN:
            case STACK_ORIGIN:
            case SEARCHABLE_SNAPSHOTS_ORIGIN:
            case LOGSTASH_MANAGEMENT_ORIGIN:
            case FLEET_ORIGIN:
            case ENT_SEARCH_ORIGIN:
            case CONNECTORS_ORIGIN:
            case INFERENCE_ORIGIN:
            case ESQL_ORIGIN:
            case TASKS_ORIGIN:   // TODO use a more limited user for tasks
                securityContext.executeAsInternalUser(InternalUsers.XPACK_USER, version, consumer);
                break;
            case ASYNC_SEARCH_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.ASYNC_SEARCH_USER, version, consumer);
                break;
            case SYNONYMS_ORIGIN:
                securityContext.executeAsInternalUser(InternalUsers.SYNONYMS_USER, version, consumer);
                break;
            default:
                assert false : "action.origin [" + actionOrigin + "] is unknown!";
                throw new IllegalStateException("action.origin [" + actionOrigin + "] should always be a known value");
        }
    }

    private static boolean isInternalAction(String action) {
        return INTERNAL_PREDICATE.test(action);
    }
}
