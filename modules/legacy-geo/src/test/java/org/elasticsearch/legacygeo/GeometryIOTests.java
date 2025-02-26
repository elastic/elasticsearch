/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.legacygeo;

import org.elasticsearch.common.geo.GeometryIO;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.geo.GeometryTestUtils.randomGeometry;
import static org.elasticsearch.legacygeo.query.LegacyGeoShapeQueryProcessor.geometryToShapeBuilder;

public class GeometryIOTests extends ESTestCase {

    public void testRandomSerialization() throws Exception {
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            boolean hasAlt = randomBoolean();
            Geometry geometry = randomGeometry(hasAlt);
            if (shapeSupported(geometry) && randomBoolean()) {
                // Shape builder conversion doesn't support altitude
                ShapeBuilder<?, ?, ?> shapeBuilder = geometryToShapeBuilder(geometry);
                if (randomBoolean()) {
                    Geometry actual = shapeBuilder.buildGeometry();
                    assertEquals(geometry, actual);
                }
                if (randomBoolean()) {
                    // Test ShapeBuilder -> Geometry Serialization
                    try (BytesStreamOutput out = new BytesStreamOutput()) {
                        out.writeNamedWriteable(shapeBuilder);
                        try (StreamInput in = out.bytes().streamInput()) {
                            Geometry actual = GeometryIO.readGeometry(in);
                            assertEquals(geometry, actual);
                            assertEquals(0, in.available());
                        }
                    }
                } else {
                    // Test Geometry -> ShapeBuilder Serialization
                    try (BytesStreamOutput out = new BytesStreamOutput()) {
                        GeometryIO.writeGeometry(out, geometry);
                        try (StreamInput in = out.bytes().streamInput()) {
                            try (StreamInput nin = new NamedWriteableAwareStreamInput(in, this.writableRegistry())) {
                                ShapeBuilder<?, ?, ?> actual = nin.readNamedWriteable(ShapeBuilder.class);
                                assertEquals(shapeBuilder, actual);
                                assertEquals(0, in.available());
                            }
                        }
                    }
                }
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

    private boolean shapeSupported(Geometry geometry) {
        if (geometry.hasZ()) {
            return false;
        }

        if (geometry.type() == ShapeType.GEOMETRYCOLLECTION) {
            GeometryCollection<?> collection = (GeometryCollection<?>) geometry;
            for (Geometry g : collection) {
                if (shapeSupported(g) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(GeoShapeType.getShapeWriteables());
    }
}
