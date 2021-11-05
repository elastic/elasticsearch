/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.geo.GeometryTestUtils.randomGeometry;

public class GeometryIOTests extends ESTestCase {

    public void testRandomSerialization() throws Exception {
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            boolean hasAlt = randomBoolean();
            Geometry geometry = randomGeometry(hasAlt);
            // Test Geometry -> Geometry
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                GeometryIO.writeGeometry(out, geometry);
                ;
                try (StreamInput in = out.bytes().streamInput()) {
                    Geometry actual = GeometryIO.readGeometry(in);
                    assertEquals(geometry, actual);
                    assertEquals(0, in.available());
                }
            }
        }
    }
}
