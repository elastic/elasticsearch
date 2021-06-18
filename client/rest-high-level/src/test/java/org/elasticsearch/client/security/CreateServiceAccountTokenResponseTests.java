/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class CreateServiceAccountTokenResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse,
    CreateServiceAccountTokenResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse createServerTestInstance(
        XContentType xContentType) {
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final String value = randomAlphaOfLength(22);
        return org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse.created(
            tokenName, new SecureString(value.toCharArray()));
    }

    @Override
    protected CreateServiceAccountTokenResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return CreateServiceAccountTokenResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse serverTestInstance,
        CreateServiceAccountTokenResponse clientInstance) {
        assertThat(serverTestInstance.getName(), equalTo(clientInstance.getName()));
        assertThat(serverTestInstance.getValue(), equalTo(clientInstance.getValue()));
    }
}
