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

import org.elasticsearch.test.ESTestCase;

public class MedianTests extends ESTestCase {

    public void testAVL() {
        double[] data = new double[] { 7, 15, 36, 39, 40, 41 };
        TDigest digest = new AVLTreeDigest(100);
        for (double value : data) {
            digest.add(value);
        }

        assertEquals(37.5, digest.quantile(0.5), 0);
        assertEquals(0.5, digest.cdf(37.5), 0);
    }

    public void testMergingDigest() {
        double[] data = new double[] { 7, 15, 36, 39, 40, 41 };
        TDigest digest = new MergingDigest(100);
        for (double value : data) {
            digest.add(value);
        }

        assertEquals(37.5, digest.quantile(0.5), 0);
        assertEquals(0.5, digest.cdf(37.5), 0);
    }

    public void testSortingDigest() {
        double[] data = new double[] { 7, 15, 36, 39, 40, 41 };
        TDigest digest = new SortingDigest();
        for (double value : data) {
            digest.add(value);
        }

        assertEquals(37.5, digest.quantile(0.5), 0);
        assertEquals(0.5, digest.cdf(37.5), 0);
    }

    public void testHybridDigest() {
        double[] data = new double[] { 7, 15, 36, 39, 40, 41 };
        TDigest digest = new HybridDigest(100);
        for (double value : data) {
            digest.add(value);
        }

        assertEquals(37.5, digest.quantile(0.5), 0);
        assertEquals(0.5, digest.cdf(37.5), 0);
    }

    public void testReferenceWikipedia() {
        double[] data = new double[] { 7, 15, 36, 39, 40, 41 };
        assertEquals(37.5, Dist.quantile(0.5, data), 0);
        assertEquals(0.5, Dist.cdf(37.5, data), 0);
    }
}
