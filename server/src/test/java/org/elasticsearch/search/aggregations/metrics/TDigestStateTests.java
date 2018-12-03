/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class TDigestStateTests extends ESTestCase {

    public void testMoreThan4BValues() {
        // Regression test for #19528
        // See https://github.com/tdunning/t-digest/pull/70/files#diff-4487072cee29b939694825647928f742R439
        TDigestState digest = new TDigestState(100);
        for (int i = 0; i < 1000; ++i) {
            digest.add(randomDouble());
        }
        final int count = 1 << 29;
        for (int i = 0; i < 10; ++i) {
            digest.add(randomDouble(), count);
        }
        assertEquals(1000 + 10L * (1 << 29), digest.size());
        assertTrue(digest.size() > 2 * Integer.MAX_VALUE);
        final double[] quantiles = new double[] { 0, 0.1, 0.5, 0.9, 1, randomDouble()};
        Arrays.sort(quantiles);
        double prev = Double.NEGATIVE_INFINITY;
        for (double q : quantiles) {
            final double v = digest.quantile(q);
            logger.trace("q=" + q + ", v=" + v);
            assertTrue(v >= prev);
            assertTrue("Unexpectedly low value: " + v, v >= 0.0);
            assertTrue("Unexpectedly high value: " + v, v <= 1.0);
            prev = v;
        }
    }
}
