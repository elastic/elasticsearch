/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

public abstract class BigCountTests extends TDigestTestCase {

    public void testBigMerge() {
        try (TDigest digest = createDigest()) {
            for (int i = 0; i < 5; i++) {
                try (TDigest digestToMerge = getDigest()) {
                    digest.add(digestToMerge);
                }
                double actual = digest.quantile(0.5);
                assertEquals("Count = " + digest.size(), 3000, actual, 0.001);
            }
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
