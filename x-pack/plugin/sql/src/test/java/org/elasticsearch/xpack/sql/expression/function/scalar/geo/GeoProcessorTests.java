/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor.GeoOperation;

import java.io.IOException;
import java.util.Locale;

public class GeoProcessorTests extends AbstractWireSerializingTestCase<GeoProcessor> {
    public static GeoProcessor randomGeoProcessor() {
        return new GeoProcessor(randomFrom(GeoOperation.values()));
    }

    @Override
    protected GeoProcessor createTestInstance() {
        return randomGeoProcessor();
    }

    @Override
    protected Reader<GeoProcessor> instanceReader() {
        return GeoProcessor::new;
    }

    @Override
    protected GeoProcessor mutateInstance(GeoProcessor instance) throws IOException {
        return new GeoProcessor(randomValueOtherThan(instance.processor(), () -> randomFrom(GeoOperation.values())));
    }

    public void testAsWKTApply() throws Exception {
        GeoProcessor proc = new GeoProcessor(GeoOperation.ASWKT);
        assertNull(proc.process(null));
        assertEquals("point (10.0 20.0)", proc.process(new GeoPoint(20, 10)));

        assertNull(proc.process(null));
        assertEquals("point (10.0 20.0)", proc.process(new GeoShape("POINT (10 20)")));
    }

    public void testAsWKTTypeCheck() {
        GeoProcessor proc = new GeoProcessor(GeoOperation.ASWKT);
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process("string"));
        assertEquals("A geo_shape is required; received [string]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process(42));
        assertEquals("A geo_shape is required; received [42]", siae.getMessage());
    }

    public void testAllTypesAreProcessed()  {
        for (GeoShapeType type : GeoShapeType.values()) {
            String geometryType = GeoShape.geometryType(type);
            assertEquals(geometryType.toUpperCase(Locale.ROOT), type.shapeName().toUpperCase(Locale.ROOT));
        }
    }
}
