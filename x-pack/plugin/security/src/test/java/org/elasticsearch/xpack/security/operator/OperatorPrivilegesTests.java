/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.DefaultOperatorPrivilegesService;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.junit.Before;

import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.NOOP_OPERATOR_PRIVILEGES_SERVICE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class OperatorPrivilegesTests extends ESTestCase {

    private XPackLicenseState xPackLicenseState;
    private FileOperatorUsersStore fileOperatorUsersStore;
    private OperatorOnlyRegistry operatorOnlyRegistry;

    @Before
    public void init() {
        xPackLicenseState = mock(XPackLicenseState.class);
        fileOperatorUsersStore = mock(FileOperatorUsersStore.class);
        operatorOnlyRegistry = mock(OperatorOnlyRegistry.class);
    }

    public void testWillNotProcessWhenFeatureIsDisabledOrLicenseDoesNotSupport() {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", randomBoolean())
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(false);

        final OperatorPrivilegesService operatorPrivilegesService =
            new DefaultOperatorPrivilegesService(xPackLicenseState, fileOperatorUsersStore, operatorOnlyRegistry);
        final ThreadContext threadContext = new ThreadContext(settings);

        operatorPrivilegesService.maybeMarkOperatorUser(mock(Authentication.class), threadContext);
        verifyZeroInteractions(fileOperatorUsersStore);

        final ElasticsearchSecurityException e =
            operatorPrivilegesService.check("cluster:action", mock(TransportRequest.class), threadContext);
        assertNull(e);
        verifyZeroInteractions(operatorOnlyRegistry);
    }

    public void testMarkOperatorUser() {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", true)
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(true);
        final Authentication operatorAuth = mock(Authentication.class);
        final Authentication nonOperatorAuth = mock(Authentication.class);
        when(operatorAuth.getUser()).thenReturn(new User("operator_user"));
        when(fileOperatorUsersStore.isOperatorUser(operatorAuth)).thenReturn(true);
        when(fileOperatorUsersStore.isOperatorUser(nonOperatorAuth)).thenReturn(false);

        final OperatorPrivilegesService operatorPrivilegesService =
            new DefaultOperatorPrivilegesService(xPackLicenseState, fileOperatorUsersStore, operatorOnlyRegistry);
        ThreadContext threadContext = new ThreadContext(settings);

        operatorPrivilegesService.maybeMarkOperatorUser(operatorAuth, threadContext);
        assertEquals(AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR,
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));

        threadContext = new ThreadContext(settings);
        operatorPrivilegesService.maybeMarkOperatorUser(nonOperatorAuth, threadContext);
        assertNull(threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));
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

        final OperatorPrivilegesService operatorPrivilegesService =
            new DefaultOperatorPrivilegesService(xPackLicenseState, fileOperatorUsersStore, operatorOnlyRegistry);

        ThreadContext threadContext = new ThreadContext(settings);
        if (randomBoolean()) {
            threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
            assertNull(operatorPrivilegesService.check(operatorAction, mock(TransportRequest.class), threadContext));
        } else {
            final ElasticsearchSecurityException e = operatorPrivilegesService.check(
                operatorAction, mock(TransportRequest.class), threadContext);
            assertNotNull(e);
            assertThat(e.getMessage(), containsString("Operator privileges are required for " + message));
        }

        assertNull(operatorPrivilegesService.check(nonOperatorAction, mock(TransportRequest.class), threadContext));
    }

    public void testNoOpService() {
        final Authentication authentication = mock(Authentication.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        NOOP_OPERATOR_PRIVILEGES_SERVICE.maybeMarkOperatorUser(authentication, threadContext);
        verifyZeroInteractions(authentication);
        assertNull(threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));

        final TransportRequest request = mock(TransportRequest.class);
        assertNull(NOOP_OPERATOR_PRIVILEGES_SERVICE.check(
            randomAlphaOfLengthBetween(10, 20), request, threadContext));
        verifyZeroInteractions(request);
    }

}
