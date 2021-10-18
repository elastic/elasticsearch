/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.DefaultOperatorPrivilegesService;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.NOOP_OPERATOR_PRIVILEGES_SERVICE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class OperatorPrivilegesTests extends ESTestCase {

    private XPackLicenseState xPackLicenseState;
    private FileOperatorUsersStore fileOperatorUsersStore;
    private OperatorOnlyRegistry operatorOnlyRegistry;
    private OperatorPrivilegesService operatorPrivilegesService;

    @Before
    public void init() {
        xPackLicenseState = mock(XPackLicenseState.class);
        fileOperatorUsersStore = mock(FileOperatorUsersStore.class);
        operatorOnlyRegistry = mock(OperatorOnlyRegistry.class);
        operatorPrivilegesService = new DefaultOperatorPrivilegesService(xPackLicenseState, fileOperatorUsersStore, operatorOnlyRegistry);
    }

    public void testWillNotProcessWhenFeatureIsDisabledOrLicenseDoesNotSupport() {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", randomBoolean())
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(false);
        final ThreadContext threadContext = new ThreadContext(settings);

        operatorPrivilegesService.maybeMarkOperatorUser(mock(Authentication.class), threadContext);
        verifyZeroInteractions(fileOperatorUsersStore);

        final ElasticsearchSecurityException e =
            operatorPrivilegesService.check(mock(Authentication.class), "cluster:action", mock(TransportRequest.class), threadContext);
        assertNull(e);
        verifyZeroInteractions(operatorOnlyRegistry);
    }

    public void testMarkOperatorUser() throws IllegalAccessException {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", true)
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(true);
        final Authentication operatorAuth = mock(Authentication.class);
        final Authentication nonOperatorAuth = mock(Authentication.class);
        final User operatorUser = new User("operator_user");
        when(operatorAuth.getUser()).thenReturn(operatorUser);
        when(nonOperatorAuth.getUser()).thenReturn(new User("non_operator_user"));
        when(fileOperatorUsersStore.isOperatorUser(operatorAuth)).thenReturn(true);
        when(fileOperatorUsersStore.isOperatorUser(nonOperatorAuth)).thenReturn(false);
        ThreadContext threadContext = new ThreadContext(settings);

        // Will mark for the operator user
        final Logger logger = LogManager.getLogger(OperatorPrivileges.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(logger, appender);
        Loggers.setLevel(logger, Level.DEBUG);

        try {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "marking",
                    logger.getName(),
                    Level.DEBUG,
                    "Marking user [" + operatorUser + "] as an operator"
                )
            );
            operatorPrivilegesService.maybeMarkOperatorUser(operatorAuth, threadContext);
            assertEquals(AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR,
                threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
            Loggers.setLevel(logger, (Level) null);
        }

        // Will not mark for non-operator user
        threadContext = new ThreadContext(settings);
        operatorPrivilegesService.maybeMarkOperatorUser(nonOperatorAuth, threadContext);
        assertNull(threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));

        // Will not mark for run_as user
        final Authentication runAsAuth = mock(Authentication.class);
        when(runAsAuth.getUser()).thenReturn(new User(operatorUser, operatorUser));
        Mockito.reset(fileOperatorUsersStore);
        when(fileOperatorUsersStore.isOperatorUser(runAsAuth)).thenReturn(true);
        threadContext = new ThreadContext(settings);
        operatorPrivilegesService.maybeMarkOperatorUser(runAsAuth, threadContext);
        assertNull(threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));
        verify(fileOperatorUsersStore, never()).isOperatorUser(any());

        // Will not mark for internal users
        final Authentication internalAuth = mock(Authentication.class);
        when(internalAuth.getUser()).thenReturn(
            randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE));
        threadContext = new ThreadContext(settings);
        operatorPrivilegesService.maybeMarkOperatorUser(runAsAuth, threadContext);
        assertNull(threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));
        verify(fileOperatorUsersStore, never()).isOperatorUser(any());
    }

    public void testCheck() {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", true)
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(true);

        final String operatorAction = "cluster:operator_only/action";
        final String nonOperatorAction = "cluster:non_operator/action";
        final String message = "[" + operatorAction + "]";
        when(operatorOnlyRegistry.check(eq(operatorAction), any())).thenReturn(() -> message);
        when(operatorOnlyRegistry.check(eq(nonOperatorAction), any())).thenReturn(null);
        ThreadContext threadContext = new ThreadContext(settings);
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(new User(randomAlphaOfLengthBetween(3, 8)));

        if (randomBoolean()) {
            threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
            assertNull(operatorPrivilegesService.check(authentication, operatorAction, mock(TransportRequest.class), threadContext));
        } else {
            final ElasticsearchSecurityException e = operatorPrivilegesService.check(
                authentication, operatorAction, mock(TransportRequest.class), threadContext);
            assertNotNull(e);
            assertThat(e.getMessage(), containsString("Operator privileges are required for " + message));
        }

        assertNull(operatorPrivilegesService.check(authentication, nonOperatorAction, mock(TransportRequest.class), threadContext));
    }

    public void testCheckWillPassForInternalUsers() {
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(true);
        final Authentication internalAuth = mock(Authentication.class);
        when(internalAuth.getUser()).thenReturn(
            randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE));
        assertNull(operatorPrivilegesService.check(
            internalAuth, randomAlphaOfLengthBetween(20, 30), mock(TransportRequest.class), new ThreadContext(Settings.EMPTY)));
        verify(operatorOnlyRegistry, never()).check(anyString(), any());
    }

    public void testMaybeInterceptRequest() throws IllegalAccessException {
        final boolean licensed = randomBoolean();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(licensed);

        final Logger logger = LogManager.getLogger(OperatorPrivileges.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(logger, appender);
        Loggers.setLevel(logger, Level.DEBUG);

        try {
            final RestoreSnapshotRequest restoreSnapshotRequest = mock(RestoreSnapshotRequest.class);
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "intercepting",
                    logger.getName(),
                    Level.DEBUG,
                    "Intercepting [" + restoreSnapshotRequest + "] for operator privileges"
                )
            );
            operatorPrivilegesService.maybeInterceptRequest(new ThreadContext(Settings.EMPTY), restoreSnapshotRequest);
            verify(restoreSnapshotRequest).skipOperatorOnlyState(licensed);
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
            Loggers.setLevel(logger, (Level) null);
        }
    }

    public void testMaybeInterceptRequestWillNotInterceptRequestsOtherThanRestoreSnapshotRequest() {
        final TransportRequest transportRequest = mock(TransportRequest.class);
        operatorPrivilegesService.maybeInterceptRequest(new ThreadContext(Settings.EMPTY), transportRequest);
        verifyZeroInteractions(xPackLicenseState);
    }

    public void testNoOpService() {
        final Authentication authentication = mock(Authentication.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        NOOP_OPERATOR_PRIVILEGES_SERVICE.maybeMarkOperatorUser(authentication, threadContext);
        verifyZeroInteractions(authentication);
        assertNull(threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));

        final TransportRequest request = mock(TransportRequest.class);
        assertNull(NOOP_OPERATOR_PRIVILEGES_SERVICE.check(
            mock(Authentication.class), randomAlphaOfLengthBetween(10, 20), request, threadContext));
        verifyZeroInteractions(request);
    }

    public void testNoOpServiceMaybeInterceptRequest() {
        final RestoreSnapshotRequest restoreSnapshotRequest = mock(RestoreSnapshotRequest.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        NOOP_OPERATOR_PRIVILEGES_SERVICE.maybeInterceptRequest(threadContext, restoreSnapshotRequest);
        verify(restoreSnapshotRequest).skipOperatorOnlyState(false);

        // The test just makes sure that other requests are also accepted without any error
        NOOP_OPERATOR_PRIVILEGES_SERVICE.maybeInterceptRequest(threadContext, mock(TransportRequest.class));
    }

}
