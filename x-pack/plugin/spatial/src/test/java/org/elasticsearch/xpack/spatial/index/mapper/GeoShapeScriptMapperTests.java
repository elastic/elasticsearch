/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MapperScriptTestCase;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class GeoShapeScriptMapperTests extends MapperScriptTestCase<GeometryFieldScript.Factory> {

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new LocalStateSpatialPlugin());
    }

    private static GeometryFieldScript.Factory factory(Consumer<GeometryFieldScript.Emit> executor) {
        return new GeometryFieldScript.Factory() {
            @Override
            public GeometryFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                OnScriptError onScriptError
            ) {
                return new GeometryFieldScript.LeafFactory() {
                    @Override
                    public GeometryFieldScript newInstance(LeafReaderContext ctx) {
                        return new GeometryFieldScript(fieldName, params, searchLookup, OnScriptError.FAIL, ctx) {
                            @Override
                            public void execute() {
                                executor.accept(new Emit(this));
                            }
                        };
                    }
                };
            }
        };
    }

    @Override
    protected String type() {
        return GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected GeometryFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected GeometryFieldScript.Factory errorThrowingScript() {
        return factory(s -> { throw new UnsupportedOperationException("Oops"); });
    }

    @Override
    protected GeometryFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit("POINT(-1 1)"));
    }

    @Override
    protected GeometryFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit("POINT(-1 1)");
            s.emit("POINT(-2 2)");
        });
    }

    @Override
    protected void assertMultipleValues(List<IndexableField> fields) {
        assertEquals(3, fields.size());
        assertPoint(fields.get(0), -1, 1);
        assertPoint(fields.get(1), -2, 2);
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(new Point(-1, 1));
        centroidCalculator.add(new Point(-2, 2));
        assertBinaryDocValue(fields.get(2), centroidCalculator, fields.get(0), fields.get(1));
    }

    @Override
    protected void assertDocValuesDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertPoint(fields.get(0), -1, 1);
    }

    @Override
    protected void assertIndexDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        Field[] f = LatLonShape.createIndexableFields("test", 1, -1);
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(new Point(-1, 1));
        assertBinaryDocValue(fields.get(0), centroidCalculator, f);
    }

    private void assertPoint(IndexableField indexableField, double lon, double lat) {
        ShapeField.DecodedTriangle triangle = new ShapeField.DecodedTriangle();
        ShapeField.decodeTriangle(indexableField.binaryValue().bytes, triangle);
        assertEquals(triangle.aX, triangle.bX, 0.0);
        assertEquals(triangle.aY, triangle.bY, 0.0);
        assertEquals(triangle.aX, triangle.cX, 0.0);
        assertEquals(triangle.aY, triangle.cY, 0.0);
        assertEquals(triangle.aX, GeoEncodingUtils.encodeLongitude(lon), 0.0);
        assertEquals(triangle.aY, GeoEncodingUtils.encodeLatitude(lat), 0.0);
    }

    private void assertBinaryDocValue(IndexableField binaryDocValue, CentroidCalculator calculator, IndexableField... values) {
        try {
            BytesRef bytesRef = GeometryDocValueWriter.write(Arrays.stream(values).toList(), CoordinateEncoder.GEO, calculator);
            assertEquals(bytesRef, binaryDocValue.binaryValue());
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

    }
}
