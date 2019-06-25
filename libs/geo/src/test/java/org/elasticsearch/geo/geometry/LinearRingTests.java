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

package org.elasticsearch.geo.geometry;

import org.elasticsearch.geo.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;

public class LinearRingTests extends ESTestCase {

    public void testBasicSerialization() {
        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class,
            () -> new WellKnownText(true, true).toWKT(new LinearRing(new double[]{1, 2, 3, 1}, new double[]{3, 4, 5, 3})));
        assertEquals("line ring cannot be serialized using WKT", ex.getMessage());
    }

    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new LinearRing(new double[]{1, 2, 3}, new double[]{3, 4, 5}));
        assertEquals("first and last points of the linear ring must be the same (it must close itself): lats[0]=1.0 lats[2]=3.0 " +
                "lons[0]=3.0 lons[2]=5.0",
            ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> new LinearRing(new double[]{1, 2, 1}, new double[]{3, 4, 3}, new double[]{1, 2, 3}));
        assertEquals("first and last points of the linear ring must be the same (it must close itself): lats[0]=1.0 lats[2]=1.0 " +
                "lons[0]=3.0 lons[2]=3.0 alts[0]=1.0 alts[2]=3.0",
            ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new LinearRing(new double[]{1}, new double[]{3}));
        assertEquals("at least two points in the line is required", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new LinearRing(new double[]{1, 2, 3, 1}, new double[]{3, 4, 500, 3}));
        assertEquals("invalid longitude 500.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new LinearRing(new double[]{1, 100, 3, 1}, new double[]{3, 4, 5, 3}));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());
    }

    public void testVisitor() {
        BaseGeometryTestCase.testVisitor(new LinearRing(new double[]{1, 2, 3, 1}, new double[]{3, 4, 5, 3}));
    }
}
