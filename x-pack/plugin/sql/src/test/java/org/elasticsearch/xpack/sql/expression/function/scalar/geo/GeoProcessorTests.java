/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor.GeoOperation;

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

    public void testApply() throws Exception {
        GeoProcessor proc = new GeoProcessor(GeoOperation.ASWKT_POINT);
        assertNull(proc.process(null));
        assertEquals("point (10.0 20.0)", proc.process(new GeoPoint(20, 10)));

        proc = new GeoProcessor(GeoOperation.ASWKT_SHAPE);
        assertNull(proc.process(null));
        assertEquals("point (10.0 20.0)", proc.process(new GeoShape("POINT (10 20)")));
    }

    public void testTypeCheck() {
        GeoProcessor procPoint = new GeoProcessor(GeoOperation.ASWKT_POINT);
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> procPoint.process("string"));
        assertEquals("A geo_point is required; received [string]", siae.getMessage());

        GeoProcessor procShape = new GeoProcessor(GeoOperation.ASWKT_SHAPE);
        siae = expectThrows(SqlIllegalArgumentException.class, () -> procShape.process("string"));
        assertEquals("A geo_shape is required; received [string]", siae.getMessage());
    }
}
