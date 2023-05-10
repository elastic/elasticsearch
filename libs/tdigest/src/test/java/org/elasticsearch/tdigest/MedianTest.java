/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

public class MedianTest extends AbstractTest {
    @Test
    public void testAVL() {
        double[] data = new double[]{7, 15, 36, 39, 40, 41};
        TDigest digest = new AVLTreeDigest(100);
        for (double value : data) {
            digest.add(value);
        }

        assertEquals(37.5, digest.quantile(0.5), 0);
    }

    @Test
    public void testMergingDigest() {
        double[] data = new double[]{7, 15, 36, 39, 40, 41};
        TDigest digest = new MergingDigest(100);
        for (double value : data) {
            digest.add(value);
        }

        assertEquals(37.5, digest.quantile(0.5), 0);
    }

    @Test
    public void testReferenceWikipedia() {
        double[] data = new double[] { 7, 15, 36, 39, 40, 41 };
        assertEquals(37.5, Dist.quantile(0.5, data), 0);
    }
}
