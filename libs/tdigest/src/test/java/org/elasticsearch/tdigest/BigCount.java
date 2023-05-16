/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

public abstract class BigCount extends AbstractTest {

    @Test
    public void testBigMerge() {
        TDigest digest = createDigest();
        for (int i = 0; i < 5; i++) {
            digest.add(getDigest());
            double actual = digest.quantile(0.5);
            assertEquals("Count = " + digest.size(), 3000,
                    actual, 0.001);
        }
    }

    private TDigest getDigest() {
        TDigest digest = createDigest();
        addData(digest);
        return digest;
    }

    public TDigest createDigest() {
        throw new IllegalStateException("Should have over-ridden createDigest");
    }

    private static void addData(TDigest digest) {
        double n = 300_000_000 * 5 + 200;

        addFakeCentroids(digest, n, 300_000_000, 10);
        addFakeCentroids(digest, n, 300_000_000, 200);
        addFakeCentroids(digest, n, 300_000_000, 3000);
        addFakeCentroids(digest, n, 300_000_000, 4000);
        addFakeCentroids(digest, n, 300_000_000, 5000);
        addFakeCentroids(digest, n, 200, 47883554);

        assertEquals(n, digest.size(), 0);
    }

    private static void addFakeCentroids(TDigest digest, double n, int points, int x) {
        long base = digest.size();
        double q0 = base / n;
        long added = 0;
        while (added < points) {
            double k0 = digest.scale.k(q0, digest.compression(), n);
            double q1 = digest.scale.q(k0 + 1, digest.compression(), n);
            q1 = Math.min(q1, (base + points) / n);
            int m = (int) Math.min(points - added, Math.max(1, Math.rint((q1 - q0) * n)));
            added += m;
            digest.add(x, m);
            q0 = q1;
        }
    }
}
