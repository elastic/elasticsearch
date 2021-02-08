/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.license;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class GetBasicStatusResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.license.GetBasicStatusResponse, GetBasicStatusResponse> {

    @Override
    protected org.elasticsearch.license.GetBasicStatusResponse createServerTestInstance(XContentType xContentType) {
        return new org.elasticsearch.license.GetBasicStatusResponse(randomBoolean());
    }

    @Override
    protected GetBasicStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetBasicStatusResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.license.GetBasicStatusResponse serverTestInstance,
                                   GetBasicStatusResponse clientInstance) {
        org.elasticsearch.license.GetBasicStatusResponse serverInstance =
            new org.elasticsearch.license.GetBasicStatusResponse(clientInstance.isEligibleToStartBasic());
        assertEquals(serverTestInstance, serverInstance);
    }
}
