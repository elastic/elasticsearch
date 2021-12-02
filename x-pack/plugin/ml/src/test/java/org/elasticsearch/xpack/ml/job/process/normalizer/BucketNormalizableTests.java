/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.junit.Before;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class BucketNormalizableTests extends ESTestCase {
    private static final String INDEX_NAME = "foo-index";
    private static final double EPSILON = 0.0001;
    private Bucket bucket;

    @Before
    public void setUpBucket() {
        bucket = new Bucket("foo", new Date(), 600);

        BucketInfluencer bucketInfluencer1 = new BucketInfluencer("foo", bucket.getTimestamp(), 600);
        bucketInfluencer1.setInfluencerFieldName(BucketInfluencer.BUCKET_TIME);
        bucketInfluencer1.setAnomalyScore(42.0);
        bucketInfluencer1.setProbability(0.01);

        BucketInfluencer bucketInfluencer2 = new BucketInfluencer("foo", bucket.getTimestamp(), 600);
        bucketInfluencer2.setInfluencerFieldName("foo");
        bucketInfluencer2.setAnomalyScore(88.0);
        bucketInfluencer2.setProbability(0.001);

        bucket.setBucketInfluencers(Arrays.asList(bucketInfluencer1, bucketInfluencer2));

        bucket.setAnomalyScore(88.0);

        AnomalyRecord record1 = new AnomalyRecord("foo", bucket.getTimestamp(), 600);
        record1.setRecordScore(1.0);
        AnomalyRecord record2 = new AnomalyRecord("foo", bucket.getTimestamp(), 600);
        record2.setRecordScore(2.0);
        bucket.setRecords(Arrays.asList(record1, record2));
    }

    public void testIsContainerOnly() {
        assertTrue(new BucketNormalizable(bucket, INDEX_NAME).isContainerOnly());
    }

    public void testGetLevel() {
        assertEquals(Level.ROOT, new BucketNormalizable(bucket, INDEX_NAME).getLevel());
    }

    public void testGetPartitionFieldName() {
        assertNull(new BucketNormalizable(bucket, INDEX_NAME).getPartitionFieldName());
    }

    public void testGetPartitionFieldValue() {
        assertNull(new BucketNormalizable(bucket, INDEX_NAME).getPartitionFieldValue());
    }

    public void testGetPersonFieldName() {
        assertNull(new BucketNormalizable(bucket, INDEX_NAME).getPersonFieldName());
    }

    public void testGetPersonFieldValue() {
        assertNull(new BucketNormalizable(bucket, INDEX_NAME).getPersonFieldValue());
    }

    public void testGetFunctionName() {
        assertNull(new BucketNormalizable(bucket, INDEX_NAME).getFunctionName());
    }

    public void testGetValueFieldName() {
        assertNull(new BucketNormalizable(bucket, INDEX_NAME).getValueFieldName());
    }

    public void testGetProbability() {
        expectThrows(UnsupportedOperationException.class, () -> new BucketNormalizable(bucket, INDEX_NAME).getProbability());
    }

    public void testGetNormalizedScore() {
        assertEquals(88.0, new BucketNormalizable(bucket, INDEX_NAME).getNormalizedScore(), EPSILON);
    }

    public void testSetNormalizedScore() {
        BucketNormalizable normalizable = new BucketNormalizable(bucket, INDEX_NAME);

        normalizable.setNormalizedScore(99.0);

        assertEquals(99.0, normalizable.getNormalizedScore(), EPSILON);
        assertEquals(99.0, bucket.getAnomalyScore(), EPSILON);
    }

    public void testGetChildren() {
        BucketNormalizable bn = new BucketNormalizable(bucket, INDEX_NAME);

        List<Normalizable> children = bn.getChildren();
        assertEquals(2, children.size());
        assertTrue(children.get(0) instanceof BucketInfluencerNormalizable);
        assertEquals(42.0, children.get(0).getNormalizedScore(), EPSILON);
        assertTrue(children.get(1) instanceof BucketInfluencerNormalizable);
        assertEquals(88.0, children.get(1).getNormalizedScore(), EPSILON);
    }

    public void testGetChildren_GivenTypeBucketInfluencer() {
        BucketNormalizable bn = new BucketNormalizable(bucket, INDEX_NAME);
        List<Normalizable> children = bn.getChildren(Normalizable.ChildType.BUCKET_INFLUENCER);

        assertEquals(2, children.size());
        assertTrue(children.get(0) instanceof BucketInfluencerNormalizable);
        assertEquals(42.0, children.get(0).getNormalizedScore(), EPSILON);
        assertTrue(children.get(1) instanceof BucketInfluencerNormalizable);
        assertEquals(88.0, children.get(1).getNormalizedScore(), EPSILON);
    }

    public void testSetMaxChildrenScore_GivenDifferentScores() {
        BucketNormalizable bucketNormalizable = new BucketNormalizable(bucket, INDEX_NAME);

        assertTrue(bucketNormalizable.setMaxChildrenScore(Normalizable.ChildType.BUCKET_INFLUENCER, 95.0));

        assertEquals(95.0, bucket.getAnomalyScore(), EPSILON);
    }

    public void testSetMaxChildrenScore_GivenSameScores() {
        BucketNormalizable bucketNormalizable = new BucketNormalizable(bucket, INDEX_NAME);

        assertFalse(bucketNormalizable.setMaxChildrenScore(Normalizable.ChildType.BUCKET_INFLUENCER, 88.0));

        assertEquals(88.0, bucket.getAnomalyScore(), EPSILON);
    }

    public void testSetParentScore() {
        expectThrows(UnsupportedOperationException.class, () -> new BucketNormalizable(bucket, INDEX_NAME).setParentScore(42.0));
    }

    public void testResetBigChangeFlag() {
        BucketNormalizable normalizable = new BucketNormalizable(bucket, INDEX_NAME);
        normalizable.raiseBigChangeFlag();

        normalizable.resetBigChangeFlag();

        assertFalse(normalizable.hadBigNormalizedUpdate());
    }

    public void testRaiseBigChangeFlag() {
        BucketNormalizable normalizable = new BucketNormalizable(bucket, INDEX_NAME);
        normalizable.resetBigChangeFlag();

        normalizable.raiseBigChangeFlag();

        assertTrue(normalizable.hadBigNormalizedUpdate());
    }
}
