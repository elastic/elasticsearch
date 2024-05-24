/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestPutUserActionTests extends ESTestCase {

    public void testFailureWhenNativeUsersDisabled() throws Exception {
        final Settings securityDisabledSettings = Settings.builder().put(NativeRealmSettings.NATIVE_USERS_ENABLED, false).build();
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.BASIC);
        final RestPutUserAction action = new RestPutUserAction(securityDisabledSettings, licenseState);
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY) //
            .withParams(Map.of("username", "you-sir-naim"))
            .withContent(new BytesArray("{ \"password\":\"blah-blah-blah\" }"), XContentType.JSON)
            .build();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        try (var threadPool = createThreadPool()) {
            final var nodeClient = new NoOpNodeClient(threadPool);
            action.handleRequest(request, channel, nodeClient);
        }

        assertThat(channel.capturedResponse(), notNullValue());
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.GONE));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Native user management is not enabled"));
    }

}
