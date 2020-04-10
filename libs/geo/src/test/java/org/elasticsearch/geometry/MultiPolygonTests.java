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
import java.util.Collections;
import java.util.List;

public class MultiPolygonTests extends BaseGeometryTestCase<MultiPolygon> {

    @Override
    protected MultiPolygon createTestInstance(boolean hasAlt) {
        int size = randomIntBetween(1, 10);
        List<Polygon> arr = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            arr.add(GeometryTestUtils.randomPolygon(hasAlt));
        }
        return new MultiPolygon(arr);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("MULTIPOLYGON (((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0)))",
            wkt.toWKT(new MultiPolygon(Collections.singletonList(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}))))));
        assertEquals(new MultiPolygon(Collections.singletonList(
            new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1})))),
            wkt.fromWKT("MULTIPOLYGON (((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0)))"));

        assertEquals("MULTIPOLYGON EMPTY", wkt.toWKT(MultiPolygon.EMPTY));
        assertEquals(MultiPolygon.EMPTY, wkt.fromWKT("MULTIPOLYGON EMPTY)"));
    }

    public void testValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new MultiPolygon(Collections.singletonList(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 2, 3, 1}))
            ))));
        assertEquals("found Z value [1.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(
            new MultiPolygon(Collections.singletonList(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 2, 3, 1})))));
    }
}
