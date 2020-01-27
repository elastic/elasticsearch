/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor.GeoOperation;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;

import java.io.IOException;

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

    public void testApplyAsWKT() throws Exception {
        assertEquals("POINT (10.0 20.0)", new GeoProcessor(GeoOperation.ASWKT).process(new GeoShape(10, 20)));
        assertEquals("POINT (10.0 20.0)", new GeoProcessor(GeoOperation.ASWKT).process(new GeoShape("POINT (10 20)")));
    }

    public void testApplyGeometryType() throws Exception {
        assertEquals("POINT", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(new GeoShape(10, 20)));
        assertEquals("POINT", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(new GeoShape("POINT (10 20)")));
        assertEquals("MULTIPOINT", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(new GeoShape("multipoint (2.0 1.0)")));
        assertEquals("LINESTRING", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(new GeoShape("LINESTRING (3.0 1.0, 4.0 2.0)")));
        assertEquals("POLYGON", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(
            new GeoShape("polygon ((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0))")));
        assertEquals("MULTILINESTRING", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(
            new GeoShape("multilinestring ((3.0 1.0, 4.0 2.0), (2.0 1.0, 5.0 6.0))")));
        assertEquals("MULTIPOLYGON", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(
            new GeoShape("multipolygon (((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0)))")));
        assertEquals("ENVELOPE", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(new GeoShape("bbox (10.0, 20.0, 40.0, 30.0)")));
        assertEquals("GEOMETRYCOLLECTION", new GeoProcessor(GeoOperation.GEOMETRY_TYPE).process(
            new GeoShape("geometrycollection (point (20.0 10.0),point (1.0 2.0))")));
    }


    public void testApplyGetXYZ() throws Exception {
        assertEquals(10.0, new GeoProcessor(GeoOperation.X).process(new GeoShape(10, 20)));
        assertEquals(20.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape(10, 20)));
        assertNull(new GeoProcessor(GeoOperation.Z).process(new GeoShape(10, 20)));
        assertEquals(10.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("POINT (10 20)")));
        assertEquals(20.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("POINT (10 20)")));
        assertEquals(10.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("POINT (10 20 30)")));
        assertEquals(20.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("POINT (10 20 30)")));
        assertEquals(30.0, new GeoProcessor(GeoOperation.Z).process(new GeoShape("POINT (10 20 30)")));
        assertEquals(2.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("multipoint (2.0 1.0)")));
        assertEquals(1.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("multipoint (2.0 1.0)")));
        assertEquals(3.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("LINESTRING (3.0 1.0, 4.0 2.0)")));
        assertEquals(1.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("LINESTRING (3.0 1.0, 4.0 2.0)")));
        assertEquals(3.0, new GeoProcessor(GeoOperation.X).process(
            new GeoShape("multilinestring ((3.0 1.0, 4.0 2.0), (2.0 1.0, 5.0 6.0))")));
        assertEquals(1.0, new GeoProcessor(GeoOperation.Y).process(
            new GeoShape("multilinestring ((3.0 1.0, 4.0 2.0), (2.0 1.0, 5.0 6.0))")));
        //           minX                                                               minX, maxX, maxY, minY
        assertEquals(10.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("bbox (10.0, 20.0, 40.0, 30.0)")));
        //           minY                                                               minX, maxX, maxY, minY
        assertEquals(30.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("bbox (10.0, 20.0, 40.0, 30.0)")));
        assertEquals(20.0, new GeoProcessor(GeoOperation.X).process(
            new GeoShape("geometrycollection (point (20.0 10.0),point (1.0 2.0))")));
        assertEquals(10.0, new GeoProcessor(GeoOperation.Y).process(
            new GeoShape("geometrycollection (point (20.0 10.0),point (1.0 2.0))")));
    }

    public void testApplyGetXYZToPolygons() throws Exception {
        assertEquals(3.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("polygon ((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0))")));
        assertEquals(1.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("polygon ((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0))")));
        assertNull(new GeoProcessor(GeoOperation.Z).process(new GeoShape("polygon ((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0))")));
        assertEquals(5.0, new GeoProcessor(GeoOperation.Z).process(
            new GeoShape("polygon ((3.0 1.0 5.0, 4.0 2.0 6.0, 4.0 3.0 7.0, 3.0 1.0 5.0))")));
        assertEquals(3.0, new GeoProcessor(GeoOperation.X).process(new GeoShape("multipolygon (((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0)))")));
        assertEquals(1.0, new GeoProcessor(GeoOperation.Y).process(new GeoShape("multipolygon (((3.0 1.0, 4.0 2.0, 4.0 3.0, 3.0 1.0)))")));
    }

    public void testApplyNull() {
        for (GeoOperation op : GeoOperation.values()) {
            GeoProcessor proc = new GeoProcessor(op);
            assertNull(proc.process(null));
        }
    }

    public void testTypeCheck() {
        GeoProcessor proc = new GeoProcessor(GeoOperation.ASWKT);
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process("string"));
        assertEquals("A geo_point or geo_shape is required; received [string]", siae.getMessage());
    }
}
