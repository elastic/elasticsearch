/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
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
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.junit.Test;

import static org.junit.Assert.*;

public class ZeroBucketTests {

    @Test
    public void testFromThresholdLazyIndex() {
        ZeroBucket z = ZeroBucket.fromThreshold(1.25d, 5L);
        assertTrue(z.isThresholdComputed());
        assertFalse(z.isIndexComputed());
        assertEquals(1.25d, z.zeroThreshold(), 0.0);
        z.index(); // compute index
        assertTrue(z.isIndexComputed());
    }

    @Test
    public void testFromIndexLazyThreshold() {
        ZeroBucket z = ZeroBucket.fromIndexAndScale(42L, ExponentialHistogram.MAX_SCALE, 3L);
        assertTrue(z.isIndexComputed());
        assertFalse(z.isThresholdComputed());
        double thr = z.zeroThreshold();
        assertTrue(thr >= 0.0);
        assertTrue(z.isThresholdComputed());
    }

    @Test
    public void testEqualityAndHashStable() {
        ZeroBucket a = ZeroBucket.fromThreshold(0.6d, 10L);
        ZeroBucket b = ZeroBucket.fromThreshold(0.6d, 10L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        a.index(); // force index
        assertEquals(a, b);
        b.index();
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testMinimalEmptySingleton() {
        ZeroBucket m1 = ZeroBucket.minimalEmpty();
        ZeroBucket m2 = ZeroBucket.minimalEmpty();
        assertSame(m1, m2);
        assertEquals(0L, m1.count());
        assertEquals(0.0, m1.zeroThreshold(), 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNegativeThreshold() {
        ZeroBucket.fromThreshold(-0.01d, 1L);
    }

    @Test
    public void testToStringContainsKeyFields() {
        ZeroBucket z = ZeroBucket.fromThreshold(0.75d, 2L);
        String s = z.toString();
        assertTrue(s.contains("scale="));
        assertTrue(s.contains("count=2"));
    }
}