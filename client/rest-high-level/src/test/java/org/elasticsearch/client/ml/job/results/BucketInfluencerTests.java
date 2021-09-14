/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

public class BucketInfluencerTests extends AbstractXContentTestCase<BucketInfluencer> {

    @Override
    protected BucketInfluencer createTestInstance() {
        BucketInfluencer bucketInfluencer = new BucketInfluencer(randomAlphaOfLengthBetween(1, 20), new Date(randomNonNegativeLong()),
                randomNonNegativeLong());
        if (randomBoolean()) {
            bucketInfluencer.setAnomalyScore(randomDouble());
        }
        if (randomBoolean()) {
            bucketInfluencer.setInfluencerFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            bucketInfluencer.setInitialAnomalyScore(randomDouble());
        }
        if (randomBoolean()) {
            bucketInfluencer.setProbability(randomDouble());
        }
        if (randomBoolean()) {
            bucketInfluencer.setRawAnomalyScore(randomDouble());
        }
        if (randomBoolean()) {
            bucketInfluencer.setIsInterim(randomBoolean());
        }
        return bucketInfluencer;
    }

    @Override
    protected BucketInfluencer doParseInstance(XContentParser parser) {
        return BucketInfluencer.PARSER.apply(parser, null);
    }

    public void testEquals_GivenNull() {
        assertFalse(new BucketInfluencer(randomAlphaOfLengthBetween(1, 20), new Date(), 600).equals(null));
    }

    public void testEquals_GivenDifferentClass() {
        assertFalse(new BucketInfluencer(randomAlphaOfLengthBetween(1, 20), new Date(), 600).equals("a string"));
    }

    public void testEquals_GivenEqualInfluencers() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer1.setAnomalyScore(42.0);
        bucketInfluencer1.setInfluencerFieldName("foo");
        bucketInfluencer1.setInitialAnomalyScore(67.3);
        bucketInfluencer1.setProbability(0.0003);
        bucketInfluencer1.setRawAnomalyScore(3.14);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer2.setAnomalyScore(42.0);
        bucketInfluencer2.setInfluencerFieldName("foo");
        bucketInfluencer2.setInitialAnomalyScore(67.3);
        bucketInfluencer2.setProbability(0.0003);
        bucketInfluencer2.setRawAnomalyScore(3.14);

        assertTrue(bucketInfluencer1.equals(bucketInfluencer2));
        assertTrue(bucketInfluencer2.equals(bucketInfluencer1));
        assertEquals(bucketInfluencer1.hashCode(), bucketInfluencer2.hashCode());
    }

    public void testEquals_GivenDifferentAnomalyScore() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer1.setAnomalyScore(42.0);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer2.setAnomalyScore(42.1);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenDifferentFieldName() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer1.setInfluencerFieldName("foo");

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer2.setInfluencerFieldName("bar");

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenDifferentInitialAnomalyScore() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer1.setInitialAnomalyScore(42.0);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer2.setInitialAnomalyScore(42.1);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenRawAnomalyScore() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer1.setRawAnomalyScore(42.0);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer2.setRawAnomalyScore(42.1);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenDifferentProbability() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer1.setProbability(0.001);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", new Date(123), 600);
        bucketInfluencer2.setProbability(0.002);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
