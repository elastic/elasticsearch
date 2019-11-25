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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class BaseGeometryTestCase<T extends Geometry> extends AbstractWireTestCase<T> {

    @Override
    protected final T createTestInstance() {
        boolean hasAlt = randomBoolean();
        T obj = createTestInstance(hasAlt);
        assertEquals(hasAlt, obj.hasZ());
        return obj;
    }

    protected abstract T createTestInstance(boolean hasAlt);

    @Override
    protected Writeable.Reader<T> instanceReader() {
        throw new IllegalStateException("shouldn't be called in this test");
    }


    @SuppressWarnings("unchecked")
    @Override
    protected T copyInstance(T instance, Version version) throws IOException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        String text = wkt.toWKT(instance);
        try {
            return (T) wkt.fromWKT(text);
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
                return verify(collection, "GeometryCollection");            }

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
