/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.security.Security;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestProfileHasPrivilegesActionTests extends ESTestCase {
    private MockLicenseState licenseState;
    private RestProfileHasPrivilegesAction restProfileHasPrivilegesAction;

    @Before
    public void init() {
        licenseState = MockLicenseState.createMock();
        restProfileHasPrivilegesAction = new RestProfileHasPrivilegesAction(
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build(),
            mock(SecurityContext.class),
            licenseState
        );
    }

    public void testLicenseEnforcement() {
        final boolean featureAllowed = randomBoolean();
        when(licenseState.isAllowed(Security.USER_PROFILE_COLLABORATION_FEATURE)).thenReturn(featureAllowed);
        if (featureAllowed) {
            assertThat(restProfileHasPrivilegesAction.checkFeatureAvailable(new FakeRestRequest()), nullValue());
            verify(licenseState).featureUsed(Security.USER_PROFILE_COLLABORATION_FEATURE);
        } else {
            final Exception e = restProfileHasPrivilegesAction.checkFeatureAvailable(new FakeRestRequest());
            assertThat(e, instanceOf(ElasticsearchSecurityException.class));
            assertThat(e.getMessage(), containsString("current license is non-compliant for [user-profile-collaboration]"));
            assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.FORBIDDEN));
            verify(licenseState, never()).featureUsed(Security.USER_PROFILE_COLLABORATION_FEATURE);
        }
    }
}
