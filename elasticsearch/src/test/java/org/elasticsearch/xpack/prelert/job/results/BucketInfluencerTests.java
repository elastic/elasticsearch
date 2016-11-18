/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.results;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.support.AbstractSerializingTestCase;

import java.util.Date;

public class BucketInfluencerTests extends AbstractSerializingTestCase<BucketInfluencer> {

    @Override
    protected BucketInfluencer createTestInstance() {
        BucketInfluencer bucketInfluencer = new BucketInfluencer(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            bucketInfluencer.setAnomalyScore(randomDouble());
        }
        if (randomBoolean()) {
            bucketInfluencer.setInfluencerFieldName(randomAsciiOfLengthBetween(1, 20));
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
        if (randomBoolean()) {
            bucketInfluencer.setTimestamp(new Date(randomLong()));
        }
        return bucketInfluencer;
    }

    @Override
    protected Reader<BucketInfluencer> instanceReader() {
        return BucketInfluencer::new;
    }

    @Override
    protected BucketInfluencer parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return BucketInfluencer.PARSER.apply(parser, () -> matcher);
    }

    public void testEquals_GivenNull() {
        assertFalse(new BucketInfluencer(randomAsciiOfLengthBetween(1, 20)).equals(null));
    }

    public void testEquals_GivenDifferentClass() {
        assertFalse(new BucketInfluencer(randomAsciiOfLengthBetween(1, 20)).equals("a string"));
    }

    public void testEquals_GivenEqualInfluencers() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo");
        bucketInfluencer1.setAnomalyScore(42.0);
        bucketInfluencer1.setInfluencerFieldName("foo");
        bucketInfluencer1.setInitialAnomalyScore(67.3);
        bucketInfluencer1.setProbability(0.0003);
        bucketInfluencer1.setRawAnomalyScore(3.14);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo");
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
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo");
        bucketInfluencer1.setAnomalyScore(42.0);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo");
        bucketInfluencer2.setAnomalyScore(42.1);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenDifferentFieldName() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo");
        bucketInfluencer1.setInfluencerFieldName("foo");

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo");
        bucketInfluencer2.setInfluencerFieldName("bar");

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenDifferentInitialAnomalyScore() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo");
        bucketInfluencer1.setInitialAnomalyScore(42.0);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo");
        bucketInfluencer2.setInitialAnomalyScore(42.1);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenRawAnomalyScore() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo");
        bucketInfluencer1.setRawAnomalyScore(42.0);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo");
        bucketInfluencer2.setRawAnomalyScore(42.1);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

    public void testEquals_GivenDifferentProbability() {
        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo");
        bucketInfluencer1.setProbability(0.001);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo");
        bucketInfluencer2.setProbability(0.002);

        assertFalse(bucketInfluencer1.equals(bucketInfluencer2));
        assertFalse(bucketInfluencer2.equals(bucketInfluencer1));
    }

}
