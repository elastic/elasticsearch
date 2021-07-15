/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class DeleteServiceAccountTokenResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse,
    DeleteServiceAccountTokenResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse createServerTestInstance(
        XContentType xContentType) {
        return new org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse(randomBoolean());
    }

    @Override
    protected DeleteServiceAccountTokenResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return DeleteServiceAccountTokenResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse serverTestInstance,
        DeleteServiceAccountTokenResponse clientInstance) {
        assertThat(serverTestInstance.found(), is(clientInstance.isAcknowledged()));
    }
}
