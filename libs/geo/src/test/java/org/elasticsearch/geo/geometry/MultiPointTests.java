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

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiPointTests extends BaseGeometryTestCase<MultiPoint> {

    @Override
    protected MultiPoint createTestInstance() {
        int size = randomIntBetween(1, 10);
        List<Point> arr = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            arr.add(randomPoint());
        }
        return new MultiPoint(arr);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        assertEquals("multipoint (2.0 1.0)", WellKnownText.toWKT(
            new MultiPoint(Collections.singletonList(new Point(1, 2)))));
        assertEquals(new MultiPoint(Collections.singletonList(new Point(1 ,2))),
            WellKnownText.fromWKT("multipoint (2 1)"));

        assertEquals("multipoint EMPTY", WellKnownText.toWKT(MultiPoint.EMPTY));
        assertEquals(MultiPoint.EMPTY, WellKnownText.fromWKT("multipoint EMPTY)"));
    }
}
