/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class BaseGeometryTestCase<T extends Geometry> extends AbstractWireTestCase<T> {

    public static final String ZorMMustIncludeThreeValuesMsg =
        "When specifying 'Z' or 'M', coordinates must include three values. Only two coordinates were provided";

    @Override
    protected final T createTestInstance() {
        boolean hasAlt = randomBoolean();
        T obj = createTestInstance(hasAlt);
        assertEquals(hasAlt, obj.hasZ());
        return obj;
    }

    protected abstract T createTestInstance(boolean hasAlt);

    @SuppressWarnings("unchecked")
    @Override
    protected T copyInstance(T instance, TransportVersion version) throws IOException {
        String text = WellKnownText.toWKT(instance);
        try {
            return (T) WellKnownText.fromWKT(GeographyValidator.instance(true), true, text);
        } catch (ParseException e) {
            throw new ElasticsearchException(e);
        }
    }

    public void testVisitor() {
        testVisitor(createTestInstance());
    }

    public static void testVisitor(Geometry geom) {
        AtomicBoolean called = new AtomicBoolean(false);
        Object result = geom.visit(new GeometryVisitor<Object, RuntimeException>() {
            private Object verify(Geometry geometry, String expectedClass) {
                assertFalse("Visitor should be called only once", called.getAndSet(true));
                assertSame(geom, geometry);
                assertEquals(geometry.getClass().getName(), "org.elasticsearch.geometry." + expectedClass);
                return "result";
            }

            @Override
            public Object visit(Circle circle) {
                return verify(circle, "Circle");
            }

            @Override
            public Object visit(GeometryCollection<?> collection) {
                return verify(collection, "GeometryCollection");
            }

            @Override
            public Object visit(Line line) {
                return verify(line, "Line");
            }

            @Override
            public Object visit(LinearRing ring) {
                return verify(ring, "LinearRing");
            }

            @Override
            public Object visit(MultiLine multiLine) {
                return verify(multiLine, "MultiLine");
            }

            @Override
            public Object visit(MultiPoint multiPoint) {
                return verify(multiPoint, "MultiPoint");
            }

            @Override
            public Object visit(MultiPolygon multiPolygon) {
                return verify(multiPolygon, "MultiPolygon");
            }

            @Override
            public Object visit(Point point) {
                return verify(point, "Point");
            }

            @Override
            public Object visit(Polygon polygon) {
                return verify(polygon, "Polygon");
            }

            @Override
            public Object visit(Rectangle rectangle) {
                return verify(rectangle, "Rectangle");
            }
        });

        assertTrue("visitor wasn't called", called.get());
        assertEquals("result", result);
    }

}
