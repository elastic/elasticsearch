/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.junit.Before;

import java.util.Date;

public class InfluencerNormalizableTests extends ESTestCase {
    private static final String INDEX_NAME = "foo-index";
    private static final double EPSILON = 0.0001;
    private Influencer influencer;

    @Before
    public void setUpInfluencer() {
        influencer = new Influencer("foo", "airline", "AAL", new Date(), 600);
        influencer.setInfluencerScore(1.0);
        influencer.setInitialInfluencerScore(2.0);
        influencer.setProbability(0.05);
    }

    public void testIsContainerOnly() {
        assertFalse(new InfluencerNormalizable(influencer, INDEX_NAME).isContainerOnly());
    }

    public void testGetLevel() {
        assertEquals(Level.INFLUENCER, new InfluencerNormalizable(influencer, INDEX_NAME).getLevel());
    }

    public void testGetPartitionFieldName() {
        assertNull(new InfluencerNormalizable(influencer, INDEX_NAME).getPartitionFieldName());
    }

    public void testGetPartitionFieldValue() {
        assertNull(new InfluencerNormalizable(influencer, INDEX_NAME).getPartitionFieldValue());
    }

    public void testGetPersonFieldName() {
        assertEquals("airline", new InfluencerNormalizable(influencer, INDEX_NAME).getPersonFieldName());
    }

    public void testGetPersonFieldValue() {
        assertEquals("AAL", new InfluencerNormalizable(influencer, INDEX_NAME).getPersonFieldValue());
    }

    public void testGetFunctionName() {
        assertNull(new InfluencerNormalizable(influencer, INDEX_NAME).getFunctionName());
    }

    public void testGetValueFieldName() {
        assertNull(new InfluencerNormalizable(influencer, INDEX_NAME).getValueFieldName());
    }

    public void testGetProbability() {
        assertEquals(0.05, new InfluencerNormalizable(influencer, INDEX_NAME).getProbability(), EPSILON);
    }

    public void testGetNormalizedScore() {
        assertEquals(1.0, new InfluencerNormalizable(influencer, INDEX_NAME).getNormalizedScore(), EPSILON);
    }

    public void testSetNormalizedScore() {
        InfluencerNormalizable normalizable = new InfluencerNormalizable(influencer, INDEX_NAME);

        normalizable.setNormalizedScore(99.0);

        assertEquals(99.0, normalizable.getNormalizedScore(), EPSILON);
        assertEquals(99.0, influencer.getInfluencerScore(), EPSILON);
    }

    public void testGetChildrenTypes() {
        assertTrue(new InfluencerNormalizable(influencer, INDEX_NAME).getChildrenTypes().isEmpty());
    }

    public void testGetChildren_ByType() {
        expectThrows(
            UnsupportedOperationException.class,
            () -> new InfluencerNormalizable(influencer, INDEX_NAME).getChildren(Normalizable.ChildType.BUCKET_INFLUENCER)
        );
    }

    public void testGetChildren() {
        assertTrue(new InfluencerNormalizable(influencer, INDEX_NAME).getChildren().isEmpty());
    }

    public void testSetMaxChildrenScore() {
        expectThrows(
            UnsupportedOperationException.class,
            () -> new InfluencerNormalizable(influencer, INDEX_NAME).setMaxChildrenScore(Normalizable.ChildType.BUCKET_INFLUENCER, 42.0)
        );
    }

    public void testSetParentScore() {
        expectThrows(IllegalStateException.class, () -> new InfluencerNormalizable(influencer, INDEX_NAME).setParentScore(42.0));
    }

    public void testResetBigChangeFlag() {
        InfluencerNormalizable normalizable = new InfluencerNormalizable(influencer, INDEX_NAME);
        normalizable.raiseBigChangeFlag();
        normalizable.resetBigChangeFlag();
        assertFalse(normalizable.hadBigNormalizedUpdate());
    }

    public void testRaiseBigChangeFlag() {
        InfluencerNormalizable normalizable = new InfluencerNormalizable(influencer, INDEX_NAME);
        normalizable.resetBigChangeFlag();
        normalizable.raiseBigChangeFlag();
        assertTrue(normalizable.hadBigNormalizedUpdate());
    }
}
