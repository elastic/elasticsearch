/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;


import org.elasticsearch.client.license.GetBasicStatusResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;

public class GetBasicStatusResponseTests
    extends AbstractHlrcStreamableXContentTestCase<org.elasticsearch.license.GetBasicStatusResponse, GetBasicStatusResponse> {
    @Override
    public GetBasicStatusResponse doHlrcParseInstance(XContentParser parser) {
        return GetBasicStatusResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.license.GetBasicStatusResponse convertHlrcToInternal(GetBasicStatusResponse instance) {
        return new org.elasticsearch.license.GetBasicStatusResponse(instance.isEligibleToStartBasic());
    }

    @Override
    protected org.elasticsearch.license.GetBasicStatusResponse createBlankInstance() {
        return new org.elasticsearch.license.GetBasicStatusResponse(false);
    }

    @Override
    protected org.elasticsearch.license.GetBasicStatusResponse createTestInstance() {
        return new org.elasticsearch.license.GetBasicStatusResponse(randomBoolean());
    }
}
