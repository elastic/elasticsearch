/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.junit.Test;

public class L2ScoreNormalizerTests extends ESTestCase {

    @Test
    public void testNormalizeTypicalVector() {
        ScoreDoc[] docs = { new ScoreDoc(1, 3.0f, 0), new ScoreDoc(2, 4.0f, 0) };
        ScoreDoc[] normalized = L2ScoreNormalizer.INSTANCE.normalizeScores(docs);
        Assert.assertEquals(0.6f, normalized[0].score, 1e-5);
        Assert.assertEquals(0.8f, normalized[1].score, 1e-5);
    }

    @Test
    public void testAllZeros() {
        ScoreDoc[] docs = { new ScoreDoc(1, 0.0f, 0), new ScoreDoc(2, 0.0f, 0) };
        ScoreDoc[] normalized = L2ScoreNormalizer.INSTANCE.normalizeScores(docs);
        Assert.assertEquals(0.0f, normalized[0].score, 0.0f);
        Assert.assertEquals(0.0f, normalized[1].score, 0.0f);
    }

    @Test
    public void testAllNaN() {
        ScoreDoc[] docs = { new ScoreDoc(1, Float.NaN, 0), new ScoreDoc(2, Float.NaN, 0) };
        ScoreDoc[] normalized = L2ScoreNormalizer.INSTANCE.normalizeScores(docs);
        Assert.assertTrue(Float.isNaN(normalized[0].score));
        Assert.assertTrue(Float.isNaN(normalized[1].score));
    }

    @Test
    public void testMixedZeroAndNaN() {
        ScoreDoc[] docs = { new ScoreDoc(1, 0.0f, 0), new ScoreDoc(2, Float.NaN, 0) };
        ScoreDoc[] normalized = L2ScoreNormalizer.INSTANCE.normalizeScores(docs);
        Assert.assertEquals(0.0f, normalized[0].score, 0.0f);
        Assert.assertTrue(Float.isNaN(normalized[1].score));
    }

    @Test
    public void testSingleElement() {
        ScoreDoc[] docs = { new ScoreDoc(1, 42.0f, 0) };
        ScoreDoc[] normalized = L2ScoreNormalizer.INSTANCE.normalizeScores(docs);
        Assert.assertEquals(1.0f, normalized[0].score, 1e-5);
    }

    @Test
    public void testEmptyArray() {
        ScoreDoc[] docs = {};
        ScoreDoc[] normalized = L2ScoreNormalizer.INSTANCE.normalizeScores(docs);
        Assert.assertEquals(0, normalized.length);
    }
}
