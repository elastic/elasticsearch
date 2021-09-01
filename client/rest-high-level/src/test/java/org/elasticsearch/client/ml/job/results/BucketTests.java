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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

public class BucketTests extends AbstractXContentTestCase<Bucket> {

    @Override
    public Bucket createTestInstance() {
        return createTestInstance("foo");
    }

    public static Bucket createTestInstance(String jobId) {
        Bucket bucket = new Bucket(jobId, new Date(randomNonNegativeLong()), randomNonNegativeLong());
        if (randomBoolean()) {
            bucket.setAnomalyScore(randomDouble());
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<BucketInfluencer> bucketInfluencers = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                BucketInfluencer bucketInfluencer = new BucketInfluencer(jobId, new Date(), 600);
                bucketInfluencer.setAnomalyScore(randomDouble());
                bucketInfluencer.setInfluencerFieldName(randomAlphaOfLengthBetween(1, 20));
                bucketInfluencer.setInitialAnomalyScore(randomDouble());
                bucketInfluencer.setProbability(randomDouble());
                bucketInfluencer.setRawAnomalyScore(randomDouble());
                bucketInfluencers.add(bucketInfluencer);
            }
            bucket.setBucketInfluencers(bucketInfluencers);
        }
        if (randomBoolean()) {
            bucket.setEventCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            bucket.setInitialAnomalyScore(randomDouble());
        }
        if (randomBoolean()) {
            bucket.setInterim(randomBoolean());
        }
        if (randomBoolean()) {
            bucket.setProcessingTimeMs(randomLong());
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<AnomalyRecord> records = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                AnomalyRecord anomalyRecord = AnomalyRecordTests.createTestInstance(jobId);
                records.add(anomalyRecord);
            }
            bucket.setRecords(records);
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<String> scheduledEvents = new ArrayList<>(size);
            IntStream.range(0, size).forEach(i -> scheduledEvents.add(randomAlphaOfLength(20)));
            bucket.setScheduledEvents(scheduledEvents);
        }
        return bucket;
    }

    @Override
    protected Bucket doParseInstance(XContentParser parser) {
        return Bucket.PARSER.apply(parser, null);
    }

    public void testEquals_GivenDifferentClass() {
        Bucket bucket = new Bucket("foo", new Date(randomLong()), randomNonNegativeLong());
        assertFalse(bucket.equals("a string"));
    }

    public void testEquals_GivenTwoDefaultBuckets() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);

        assertTrue(bucket1.equals(bucket2));
        assertTrue(bucket2.equals(bucket1));
    }

    public void testEquals_GivenDifferentAnomalyScore() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        bucket1.setAnomalyScore(3.0);
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        bucket2.setAnomalyScore(2.0);

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenSameDates() {
        Bucket b1 = new Bucket("foo", new Date(1234567890L), 123);
        Bucket b2 = new Bucket("foo", new Date(1234567890L), 123);
        assertTrue(b1.equals(b2));
    }

    public void testEquals_GivenDifferentEventCount() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        bucket1.setEventCount(3);
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        bucket2.setEventCount(100);

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenOneHasRecordsAndTheOtherDoesNot() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        bucket1.setRecords(Collections.singletonList(new AnomalyRecord("foo", new Date(123), 123)));
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        bucket2.setRecords(Collections.emptyList());

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenDifferentNumberOfRecords() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        bucket1.setRecords(Collections.singletonList(new AnomalyRecord("foo", new Date(123), 123)));
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        bucket2.setRecords(Arrays.asList(new AnomalyRecord("foo", new Date(123), 123),
                new AnomalyRecord("foo", new Date(123), 123)));

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenSameNumberOfRecordsButDifferent() {
        AnomalyRecord anomalyRecord1 = new AnomalyRecord("foo", new Date(123), 123);
        anomalyRecord1.setRecordScore(1.0);
        AnomalyRecord anomalyRecord2 = new AnomalyRecord("foo", new Date(123), 123);
        anomalyRecord1.setRecordScore(2.0);

        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        bucket1.setRecords(Collections.singletonList(anomalyRecord1));
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        bucket2.setRecords(Collections.singletonList(anomalyRecord2));

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenDifferentIsInterim() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        bucket1.setInterim(true);
        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        bucket2.setInterim(false);

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenDifferentBucketInfluencers() {
        Bucket bucket1 = new Bucket("foo", new Date(123), 123);
        BucketInfluencer influencer1 = new BucketInfluencer("foo", new Date(123), 123);
        influencer1.setInfluencerFieldName("foo");
        bucket1.setBucketInfluencers(Collections.singletonList(influencer1));

        Bucket bucket2 = new Bucket("foo", new Date(123), 123);
        BucketInfluencer influencer2 = new BucketInfluencer("foo", new Date(123), 123);
        influencer2.setInfluencerFieldName("bar");
        bucket2.setBucketInfluencers(Collections.singletonList(influencer2));

        assertFalse(bucket1.equals(bucket2));
        assertFalse(bucket2.equals(bucket1));
    }

    public void testEquals_GivenEqualBuckets() {
        AnomalyRecord record = new AnomalyRecord("job_id", new Date(123), 123);
        BucketInfluencer bucketInfluencer = new BucketInfluencer("foo", new Date(123), 123);
        Date date = new Date();

        Bucket bucket1 = new Bucket("foo", date, 123);
        bucket1.setAnomalyScore(42.0);
        bucket1.setInitialAnomalyScore(92.0);
        bucket1.setEventCount(134);
        bucket1.setInterim(true);
        bucket1.setRecords(Collections.singletonList(record));
        bucket1.setBucketInfluencers(Collections.singletonList(bucketInfluencer));

        Bucket bucket2 = new Bucket("foo", date, 123);
        bucket2.setAnomalyScore(42.0);
        bucket2.setInitialAnomalyScore(92.0);
        bucket2.setEventCount(134);
        bucket2.setInterim(true);
        bucket2.setRecords(Collections.singletonList(record));
        bucket2.setBucketInfluencers(Collections.singletonList(bucketInfluencer));

        assertTrue(bucket1.equals(bucket2));
        assertTrue(bucket2.equals(bucket1));
        assertEquals(bucket1.hashCode(), bucket2.hashCode());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
