/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.Influencer;
import org.elasticsearch.client.ml.job.results.InfluencerTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetInfluencersResponseTests extends AbstractXContentTestCase<GetInfluencersResponse> {

    @Override
    protected GetInfluencersResponse createTestInstance() {
        String jobId = randomAlphaOfLength(20);
        int listSize = randomInt(10);
        List<Influencer> influencers = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            Influencer influencer = InfluencerTests.createTestInstance(jobId);
            influencers.add(influencer);
        }
        return new GetInfluencersResponse(influencers, listSize);
    }

    @Override
    protected GetInfluencersResponse doParseInstance(XContentParser parser) throws IOException {
        return GetInfluencersResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
