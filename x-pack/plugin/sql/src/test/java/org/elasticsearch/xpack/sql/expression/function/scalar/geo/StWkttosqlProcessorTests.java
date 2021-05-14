/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;

import static org.hamcrest.Matchers.instanceOf;

public class StWkttosqlProcessorTests extends ESTestCase {
    public static StWkttosqlProcessor randomStWkttosqlProcessor() {
        return new StWkttosqlProcessor();
    }

    public void testApply() {
        StWkttosqlProcessor proc = new StWkttosqlProcessor();
        assertNull(proc.process(null));
        Object result = proc.process("POINT (10 20)");
        assertThat(result, instanceOf(GeoShape.class));
        GeoShape geoShape = (GeoShape) result;
        assertEquals("POINT (10.0 20.0)", geoShape.toString());
    }

    public void testTypeCheck() {
        StWkttosqlProcessor procPoint = new StWkttosqlProcessor();
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> procPoint.process(42));
        assertEquals("A string is required; received [42]", siae.getMessage());

        siae = expectThrows(QlIllegalArgumentException.class, () -> procPoint.process("some random string"));
        assertEquals("Cannot parse [some random string] as a geo_shape value", siae.getMessage());

        siae = expectThrows(QlIllegalArgumentException.class, () -> procPoint.process("point (foo bar)"));
        assertEquals("Cannot parse [point (foo bar)] as a geo_shape or shape value", siae.getMessage());


        siae = expectThrows(QlIllegalArgumentException.class, () -> procPoint.process("point (10 10"));
        assertEquals("Cannot parse [point (10 10] as a geo_shape or shape value", siae.getMessage());
    }

    public void testCoerce() {
        StWkttosqlProcessor proc = new StWkttosqlProcessor();
        assertNull(proc.process(null));
        Object result = proc.process("POLYGON ((3 1 5, 4 2 4, 5 3 3))");
        assertThat(result, instanceOf(GeoShape.class));
        GeoShape geoShape = (GeoShape) result;
        assertEquals("POLYGON ((3.0 1.0 5.0, 4.0 2.0 4.0, 5.0 3.0 3.0, 3.0 1.0 5.0))", geoShape.toString());
    }
}
