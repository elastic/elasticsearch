/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
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

    /*
     * Previously we would reject requests that had a body that did not have a username set on the request. This happened because we did not
     * consume the body until after checking if there was a username set on the request. If there was not a username set on the request,
     * then the body would never be consumed. This means that the REST infrastructure would reject the request as not having a consumed body
     * despite the endpoint supporting having a body. Now, we consume the body before checking if there is a username on the request. This
     * test ensures that we maintain that behavior.
     */
    public void testBodyConsumed() throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final RestHasPrivilegesAction action =
            new RestHasPrivilegesAction(Settings.EMPTY, mock(SecurityContext.class), licenseState);
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder().startObject().endObject()) {
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withPath("/_security/user/_has_privileges/")
                .withContent(new BytesArray(bodyBuilder.toString()), XContentType.JSON)
                .build();
            final RestChannel channel = new FakeRestChannel(request, true, 1);
            action.handleRequest(request, channel, mock(NodeClient.class));
        }
    }

    public void testBasicLicense() throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final RestHasPrivilegesAction action =
            new RestHasPrivilegesAction(Settings.EMPTY, mock(SecurityContext.class), licenseState);
        when(licenseState.isSecurityAvailable()).thenReturn(false);
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder().startObject().endObject()) {
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withPath("/_security/user/_has_privileges/")
                .withContent(new BytesArray(bodyBuilder.toString()), XContentType.JSON)
                .build();
            final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
            action.handleRequest(request, channel, mock(NodeClient.class));
            assertThat(channel.capturedResponse(), notNullValue());
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.FORBIDDEN));
            assertThat(
                channel.capturedResponse().content().utf8ToString(),
                containsString("current license is non-compliant for [security]"));
        }
    }

}
