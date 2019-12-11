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

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

public class RectangleTests extends BaseGeometryTestCase<Rectangle> {
    @Override
    protected Rectangle createTestInstance(boolean hasAlt) {
        assumeFalse("3rd dimension is not supported yet", hasAlt);
        return GeometryTestUtils.randomRectangle();
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("bbox (10.0, 20.0, 40.0, 30.0)", wkt.toWKT(new Rectangle(10, 20, 40, 30)));
        assertEquals(new Rectangle(10, 20, 40, 30), wkt.fromWKT("bbox (10.0, 20.0, 40.0, 30.0)"));

        assertEquals("bbox EMPTY", wkt.toWKT(Rectangle.EMPTY));
        assertEquals(Rectangle.EMPTY, wkt.fromWKT("bbox EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = new GeographyValidator(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Rectangle(2, 3, 100, 1)));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Rectangle(200, 3, 2, 1)));
        assertEquals("invalid longitude 200.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Rectangle(2, 3, 1, 2)));
        assertEquals("max y cannot be less than min x", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Rectangle(2, 3, 2, 1, 5, Double.NaN)));
        assertEquals("only one z value is specified", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new Rectangle(50, 10, 40, 30, 20, 60)));
        assertEquals("found Z value [20.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new Rectangle(50, 10, 40, 30, 20, 60));
    }
}
