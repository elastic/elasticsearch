/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;

import static org.elasticsearch.xpack.core.security.operator.OperatorPrivilegesUtil.isOperator;

public class OperatorPrivileges {

    private static final Logger logger = LogManager.getLogger(OperatorPrivileges.class);

    public static final Setting<Boolean> OPERATOR_PRIVILEGES_ENABLED = Setting.boolSetting(
        "xpack.security.operator_privileges.enabled",
        false,
        Setting.Property.NodeScope
    );

    public interface OperatorPrivilegesService {
        /**
         * Set a ThreadContext Header {@link AuthenticationField#PRIVILEGE_CATEGORY_KEY} if authentication
         * is an operator user.
         */
        void maybeMarkOperatorUser(Authentication authentication, ThreadContext threadContext);

        /**
         * Check whether the user is an operator and whether the request is an operator-only.
         * @return An exception if user is an non-operator and the request is operator-only. Otherwise returns null.
         */
        ElasticsearchSecurityException check(
            Authentication authentication,
            String action,
            TransportRequest request,
            ThreadContext threadContext
        );

        /**
         * Checks to see if a given {@link RestHandler} is subject to operator-only restrictions for the REST API. Any REST API may be
         * fully or partially restricted. A fully restricted REST API mandates that the implementation results in
         * restChannel.sendResponse(...) and return a {@code false} to prevent any further processing. A partially restricted REST API
         * mandates that the {@link RestRequest} is marked as restricted and return {@code true}. No restrictions should also return
         * {@code true}.
         * @param restHandler The {@link RestHandler} to check for any restrictions
         * @param restRequest The {@link RestRequest} to check for any restrictions and mark any partially restricted REST API's
         * @param restChannel The {@link RestChannel} to enforce fully restricted REST API's
         * @return {@code true} if processing the request should continue, {@code false} if processing the request should halt due to
         * a fully restricted REST API
         */
        boolean checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel, ThreadContext threadContext);

        /**
         * When operator privileges are enabled, certain requests needs to be configured in a specific way
         * so that they respect operator only settings. For an example, the restore snapshot request
         * should not restore operator only states from the snapshot.
         * This method is where that requests are configured when necessary.
         */
        void maybeInterceptRequest(ThreadContext threadContext, TransportRequest request);
    }

    public static final class DefaultOperatorPrivilegesService implements OperatorPrivilegesService {

        private final FileOperatorUsersStore fileOperatorUsersStore;
        private final OperatorOnlyRegistry operatorOnlyRegistry;
        private final XPackLicenseState licenseState;

        public DefaultOperatorPrivilegesService(
            XPackLicenseState licenseState,
            FileOperatorUsersStore fileOperatorUsersStore,
            OperatorOnlyRegistry operatorOnlyRegistry
        ) {
            this.fileOperatorUsersStore = fileOperatorUsersStore;
            this.operatorOnlyRegistry = operatorOnlyRegistry;
            this.licenseState = licenseState;
        }

        public void maybeMarkOperatorUser(Authentication authentication, ThreadContext threadContext) {
            // Always mark the thread context for operator users regardless of license state which is enforced at check time
            final User user = authentication.getEffectiveSubject().getUser();
            // Let internal users pass, and mark him as an operator
            // Also check run_as, it is impossible to run_as internal users, but just to be extra safe
            // mark internalUser with operator privileges
            if (user instanceof InternalUser && false == authentication.isRunAs()) {
                if (threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY) == null) {
                    threadContext.putHeader(
                        AuthenticationField.PRIVILEGE_CATEGORY_KEY,
                        AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR
                    );
                }
                return;
            }
            // The header is already set by previous authentication either on this node or a remote node
            if (threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY) != null) {
                return;
            }
            // An operator user must not be a run_as user, it also must be recognised by the operatorUserStore
            if (false == authentication.isRunAs() && fileOperatorUsersStore.isOperatorUser(authentication)) {
                logger.debug("Marking user [{}] as an operator", user);
                threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
            } else {
                threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_EMPTY);
            }
        }

        public ElasticsearchSecurityException check(
            Authentication authentication,
            String action,
            TransportRequest request,
            ThreadContext threadContext
        ) {
            if (false == shouldProcess()) {
                return null;
            }
            final User user = authentication.getEffectiveSubject().getUser();

            // internal user is also an operator
            if (false == isOperator(threadContext)) {
                // Only check whether request is operator-only when user is NOT an operator
                logger.trace("Checking operator-only violation for user [{}] and action [{}]", user, action);
                final OperatorPrivilegesViolation violation = operatorOnlyRegistry.check(action, request);
                if (violation != null) {
                    return new ElasticsearchSecurityException("Operator privileges are required for " + violation.message());
                }
            }
            return null;
        }

        @Override
        public boolean checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel, ThreadContext threadContext) {
            if (false == shouldProcess()) {
                return true;
            }
            if (false == isOperator(threadContext)) {
                // Only check whether request is operator-only when user is NOT an operator
                if (logger.isTraceEnabled()) {
                    final User user = getUser(threadContext);
                    logger.trace("Checking for any operator-only REST violations for user [{}] and uri [{}]", user, restRequest.uri());
                }

                try {
                    operatorOnlyRegistry.checkRest(restHandler, restRequest);
                } catch (ElasticsearchException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Found the following operator-only violation [{}] for user [{}] and uri [{}]",
                            e.getMessage(),
                            getUser(threadContext),
                            restRequest.uri()
                        );
                    }
                    throw e;
                } catch (Exception e) {
                    logger.info(
                        "Unexpected exception [{}] while processing operator privileges for user [{}] and uri [{}]",
                        e.getMessage(),
                        getUser(threadContext),
                        restRequest.uri()
                    );
                    throw e;
                }
            } else {
                restRequest.markAsOperatorRequest();
                logger.trace("Marked request for uri [{}] as operator request", restRequest.uri());
            }
            return true;
        }

        private static User getUser(ThreadContext threadContext) {
            Authentication authentication = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
            return authentication.getEffectiveSubject().getUser();
        }

        public void maybeInterceptRequest(ThreadContext threadContext, TransportRequest request) {
            if (request instanceof RestoreSnapshotRequest) {
                logger.debug("Intercepting [{}] for operator privileges", request);
                ((RestoreSnapshotRequest) request).skipOperatorOnlyState(shouldProcess());
            }
        }

        private boolean shouldProcess() {
            return Security.OPERATOR_PRIVILEGES_FEATURE.check(licenseState);
        }

        // for testing
        public OperatorOnlyRegistry getOperatorOnlyRegistry() {
            return operatorOnlyRegistry;
        }
    }

    public static final OperatorPrivilegesService NOOP_OPERATOR_PRIVILEGES_SERVICE = new OperatorPrivilegesService() {
        @Override
        public void maybeMarkOperatorUser(Authentication authentication, ThreadContext threadContext) {}

        @Override
        public ElasticsearchSecurityException check(
            Authentication authentication,
            String action,
            TransportRequest request,
            ThreadContext threadContext
        ) {
            return null;
        }

        @Override
        public boolean checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel, ThreadContext threadContext) {
            return true;
        }

        @Override
        public void maybeInterceptRequest(ThreadContext threadContext, TransportRequest request) {
            if (request instanceof RestoreSnapshotRequest) {
                ((RestoreSnapshotRequest) request).skipOperatorOnlyState(false);
            }
        }
    };
}
