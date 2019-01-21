/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestHasPrivilegesActionTests extends ESTestCase {

    public void testBasicLicense() throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final RestHasPrivilegesAction action = new RestHasPrivilegesAction(Settings.EMPTY, mock(RestController.class),
            mock(SecurityContext.class), licenseState);
        when(licenseState.isSecurityAvailable()).thenReturn(false);
        final FakeRestRequest request = new FakeRestRequest();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        action.handleRequest(request, channel, mock(NodeClient.class));
        assertThat(channel.capturedResponse(), notNullValue());
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("current license is non-compliant for [security]"));
    }

}
