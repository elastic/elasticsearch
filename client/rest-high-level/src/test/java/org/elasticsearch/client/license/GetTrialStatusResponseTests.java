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

public class GetTrialStatusResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.license.GetTrialStatusResponse, GetTrialStatusResponse> {

    @Override
    protected org.elasticsearch.license.GetTrialStatusResponse createServerTestInstance(XContentType xContentType) {
        return new org.elasticsearch.license.GetTrialStatusResponse(randomBoolean());
    }

    @Override
    protected GetTrialStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetTrialStatusResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.license.GetTrialStatusResponse serverTestInstance,
                                   GetTrialStatusResponse clientInstance) {
        org.elasticsearch.license.GetTrialStatusResponse serverInstance =
            new org.elasticsearch.license.GetTrialStatusResponse(clientInstance.isEligibleToStartTrial());
        assertEquals(serverInstance, serverTestInstance);
    }
}
