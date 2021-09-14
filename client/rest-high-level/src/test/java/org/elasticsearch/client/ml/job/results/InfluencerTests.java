/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Date;

public class InfluencerTests extends AbstractXContentTestCase<Influencer> {

    public static Influencer createTestInstance(String jobId) {
        Influencer influencer = new Influencer(jobId, randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20),
                new Date(randomNonNegativeLong()), randomNonNegativeLong());
        influencer.setInterim(randomBoolean());
        influencer.setInfluencerScore(randomDouble());
        influencer.setInitialInfluencerScore(randomDouble());
        influencer.setProbability(randomDouble());
        return influencer;
    }

    @Override
    protected Influencer createTestInstance() {
        return createTestInstance(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected Influencer doParseInstance(XContentParser parser) {
        return Influencer.PARSER.apply(parser, null);
    }

    public void testToXContentDoesNotIncludeNameValueFieldWhenReservedWord() throws IOException {
        Influencer influencer = new Influencer("foo", Influencer.INFLUENCER_SCORE.getPreferredName(), "bar", new Date(), 300L);
        BytesReference bytes = XContentHelper.toXContent(influencer, XContentType.JSON, false);
        XContentParser parser = createParser(XContentType.JSON.xContent(), bytes);
        Object serialisedFieldValue = parser.map().get(Influencer.INFLUENCER_SCORE.getPreferredName());
        assertNotNull(serialisedFieldValue);
        assertNotEquals("bar", serialisedFieldValue);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
