/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilderFactory;

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
        final RestHasPrivilegesAction action = new RestHasPrivilegesAction(
            Settings.EMPTY,
            mock(SecurityContext.class),
            licenseState,
            new HasPrivilegesRequestBuilderFactory.Default()
        );
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder().startObject().endObject(); var threadPool = createThreadPool()) {
            final var client = new NoOpNodeClient(threadPool);
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_security/user/_has_privileges/")
                .withContent(new BytesArray(bodyBuilder.toString()), XContentType.JSON)
                .build();
            final RestChannel channel = new FakeRestChannel(request, true, 1);
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> action.handleRequest(request, channel, client)
            );
            assertThat(e.getMessage(), equalTo("there is no authenticated user"));
        }
    }

    public void testSecurityDisabled() throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final Settings securityDisabledSettings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.BASIC);
        final RestHasPrivilegesAction action = new RestHasPrivilegesAction(
            securityDisabledSettings,
            mock(SecurityContext.class),
            licenseState,
            new HasPrivilegesRequestBuilderFactory.Default()
        );
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder().startObject().endObject(); var threadPool = createThreadPool()) {
            final var client = new NoOpNodeClient(threadPool);
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_security/user/_has_privileges/")
                .withContent(new BytesArray(bodyBuilder.toString()), XContentType.JSON)
                .build();
            final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
            action.handleRequest(request, channel, client);
            assertThat(channel.capturedResponse(), notNullValue());
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(
                channel.capturedResponse().content().utf8ToString(),
                containsString("Security is not enabled but a security rest handler is registered")
            );
        }
    }
}
