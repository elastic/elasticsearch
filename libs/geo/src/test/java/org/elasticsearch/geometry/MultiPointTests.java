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
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiPointTests extends BaseGeometryTestCase<MultiPoint> {

    @Override
    protected MultiPoint createTestInstance(boolean hasAlt) {
        int size = randomIntBetween(1, 10);
        List<Point> arr = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            arr.add(GeometryTestUtils.randomPoint(hasAlt));
        }
        return new MultiPoint(arr);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("MULTIPOINT (2.0 1.0)", wkt.toWKT(
            new MultiPoint(Collections.singletonList(new Point(2, 1)))));
        assertEquals(new MultiPoint(Collections.singletonList(new Point(2, 1))),
            wkt.fromWKT("MULTIPOINT (2 1)"));

        assertEquals("MULTIPOINT (2.0 1.0, 3.0 4.0)",
            wkt.toWKT(new MultiPoint(Arrays.asList(new Point(2, 1), new Point(3, 4)))));
        assertEquals(new MultiPoint(Arrays.asList(new Point(2, 1), new Point(3, 4))),
            wkt.fromWKT("MULTIPOINT (2 1, 3 4)"));

        assertEquals("MULTIPOINT (2.0 1.0 10.0, 3.0 4.0 20.0)",
            wkt.toWKT(new MultiPoint(Arrays.asList(new Point(2, 1, 10), new Point(3, 4, 20)))));
        assertEquals(new MultiPoint(Arrays.asList(new Point(2, 1, 10), new Point(3, 4, 20))),
            wkt.fromWKT("MULTIPOINT (2 1 10, 3 4 20)"));

        assertEquals("MULTIPOINT EMPTY", wkt.toWKT(MultiPoint.EMPTY));
        assertEquals(MultiPoint.EMPTY, wkt.fromWKT("MULTIPOINT EMPTY)"));
    }

    public void testValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new MultiPoint(Collections.singletonList(new Point(2, 1, 3)))));
        assertEquals("found Z value [3.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new MultiPoint(Collections.singletonList(new Point(2, 1, 3))));
    }
}
