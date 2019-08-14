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

package org.elasticsearch.geometry;

import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;

public class LinearRingTests extends ESTestCase {

    public void testBasicSerialization() {
        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class,
            () -> new WellKnownText(true, new GeographyValidator(true))
                .toWKT(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1})));
        assertEquals("line ring cannot be serialized using WKT", ex.getMessage());
    }

    public void testInitValidation() {
        GeometryValidator validator = new GeographyValidator(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new LinearRing(new double[]{3, 4, 5}, new double[]{1, 2, 3})));
        assertEquals("first and last points of the linear ring must be the same (it must close itself): x[0]=3.0 x[2]=5.0 y[0]=1.0 " +
                "y[2]=3.0",
            ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new LinearRing(new double[]{3, 4, 3}, new double[]{1, 2, 1}, new double[]{1, 2, 3})));
        assertEquals("first and last points of the linear ring must be the same (it must close itself): x[0]=3.0 x[2]=3.0 y[0]=1.0 " +
                "y[2]=1.0 z[0]=1.0 z[2]=3.0",
            ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new LinearRing(new double[]{3}, new double[]{1})));
        assertEquals("at least two points in the line is required", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new LinearRing(new double[]{3, 4, 500, 3}, new double[]{1, 2, 3, 1})));
        assertEquals("invalid longitude 500.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 100, 3, 1})));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 1, 1, 1})));
        assertEquals("found Z value [1.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 1, 1, 1}));
    }

    public void testVisitor() {
        BaseGeometryTestCase.testVisitor(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}));
    }
}
