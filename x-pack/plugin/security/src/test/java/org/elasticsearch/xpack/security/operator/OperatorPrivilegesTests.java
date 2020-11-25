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
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class OperatorPrivilegesTests extends ESTestCase {

    private XPackLicenseState xPackLicenseState;
    private OperatorUserDescriptor operatorUserDescriptor;
    private OperatorOnly operatorOnly;

    @Before
    public void init() {
        xPackLicenseState = mock(XPackLicenseState.class);
        operatorUserDescriptor = mock(OperatorUserDescriptor.class);
        operatorOnly = mock(OperatorOnly.class);
    }

    public void testWillNotProcessWhenFeatureIsDisabledOrLicenseDoesNotSupport() {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", randomBoolean())
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(false);

        final OperatorPrivileges operatorPrivileges =
            new OperatorPrivileges(settings, xPackLicenseState, operatorUserDescriptor, operatorOnly);
        final ThreadContext threadContext = new ThreadContext(settings);

        operatorPrivileges.maybeMarkOperatorUser(mock(Authentication.class), threadContext);
        verifyZeroInteractions(operatorUserDescriptor);

        final ElasticsearchSecurityException e =
            operatorPrivileges.check("cluster:action", mock(TransportRequest.class), threadContext);
        assertNull(e);
        verifyZeroInteractions(operatorOnly);
    }

    public void testMarkOperatorUser() {
        final Settings settings = Settings.builder()
            .put("xpack.security.operator_privileges.enabled", true)
            .build();
        when(xPackLicenseState.checkFeature(XPackLicenseState.Feature.OPERATOR_PRIVILEGES)).thenReturn(true);
        final Authentication operatorAuth = mock(Authentication.class);
        final Authentication nonOperatorAuth = mock(Authentication.class);
        when(operatorUserDescriptor.isOperatorUser(operatorAuth)).thenReturn(true);
        when(operatorUserDescriptor.isOperatorUser(nonOperatorAuth)).thenReturn(false);

        final OperatorPrivileges operatorPrivileges =
            new OperatorPrivileges(settings, xPackLicenseState, operatorUserDescriptor, operatorOnly);
        ThreadContext threadContext = new ThreadContext(settings);

        operatorPrivileges.maybeMarkOperatorUser(operatorAuth, threadContext);
        assertEquals(AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR,
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY));

        threadContext = new ThreadContext(settings);
        operatorPrivileges.maybeMarkOperatorUser(nonOperatorAuth, threadContext);
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
        when(operatorOnly.check(eq(operatorAction), any())).thenReturn(() -> message);
        when(operatorOnly.check(eq(nonOperatorAction), any())).thenReturn(null);

        final OperatorPrivileges operatorPrivileges =
            new OperatorPrivileges(settings, xPackLicenseState, operatorUserDescriptor, operatorOnly);

        ThreadContext threadContext = new ThreadContext(settings);
        if (randomBoolean()) {
            threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
            assertNull(operatorPrivileges.check(operatorAction, mock(TransportRequest.class), threadContext));
        } else {
            final ElasticsearchSecurityException e = operatorPrivileges.check(operatorAction, mock(TransportRequest.class), threadContext);
            assertNotNull(e);
            assertThat(e.getMessage(), containsString("Operator privileges are required for " + message));
        }

        assertNull(operatorPrivileges.check(nonOperatorAction, mock(TransportRequest.class), threadContext));
    }

}
