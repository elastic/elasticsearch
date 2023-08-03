/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public abstract class AbstractRestEnterpriseSearchActionTests extends ESTestCase {
    protected void checkLicenseForRequest(FakeRestRequest request) throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final EnterpriseSearchBaseRestHandler action = getRestAction(licenseState);

        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName())) {
            action.handleRequest(request, channel, nodeClient);
        }
        assertThat(channel.capturedResponse(), notNullValue());
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Current license is non-compliant"));
    }

    protected abstract EnterpriseSearchBaseRestHandler getRestAction(XPackLicenseState licenseState);
}
