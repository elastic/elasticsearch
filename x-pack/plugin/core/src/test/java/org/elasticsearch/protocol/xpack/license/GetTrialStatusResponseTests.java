/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.client.license.GetTrialStatusResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;

public class GetTrialStatusResponseTests extends
    AbstractHlrcStreamableXContentTestCase<org.elasticsearch.license.GetTrialStatusResponse, GetTrialStatusResponse> {

    @Override
    public GetTrialStatusResponse doHlrcParseInstance(XContentParser parser) {
        return GetTrialStatusResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.license.GetTrialStatusResponse convertHlrcToInternal(GetTrialStatusResponse instance) {
        return new org.elasticsearch.license.GetTrialStatusResponse(instance.isEligibleToStartTrial());
    }

    @Override
    protected org.elasticsearch.license.GetTrialStatusResponse createBlankInstance() {
        return new org.elasticsearch.license.GetTrialStatusResponse(false);
    }

    @Override
    protected org.elasticsearch.license.GetTrialStatusResponse createTestInstance() {
        return new org.elasticsearch.license.GetTrialStatusResponse(randomBoolean());
    }
}
