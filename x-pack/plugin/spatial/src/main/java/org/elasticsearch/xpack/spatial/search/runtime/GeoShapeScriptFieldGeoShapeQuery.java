/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.runtime;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;

public class GeoShapeScriptFieldGeoShapeQuery extends AbstractGeoShapeScriptFieldQuery {

    private final Component2D component2D;
    private final LatLonGeometry[] geometries;
    private final ShapeRelation relation;
    private final GeoShapeIndexer indexer;

    public GeoShapeScriptFieldGeoShapeQuery(
        Script script,
        GeometryFieldScript.LeafFactory leafFactory,
        String fieldName,
        ShapeRelation relation,
        LatLonGeometry... geometries
    ) {
        super(script, leafFactory, fieldName);
        this.geometries = geometries;
        this.relation = relation;
        this.component2D = LatLonGeometry.create(geometries);
        this.indexer = new GeoShapeIndexer(Orientation.CCW, fieldName);
    }

    @Override
    protected boolean matches(Geometry geometry) {
        if (geometry == null) {
            return false;
        }
        final GeometryDocValueReader reader = new GeometryDocValueReader();
        final Component2DVisitor visitor = Component2DVisitor.getVisitor(component2D, relation.getLuceneRelation(), CoordinateEncoder.GEO);
        try {
            reader.reset(encodeGeometry(geometry));
            reader.visit(visitor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return visitor.matches();
    }

    private BytesRef encodeGeometry(Geometry geometry) throws IOException {
        final CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        return GeometryDocValueWriter.write(indexer.getIndexableFields(geometry), CoordinateEncoder.GEO, centroidCalculator);
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return getClass().getSimpleName();
        }
        return fieldName() + ":" + getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        GeoShapeScriptFieldGeoShapeQuery that = (GeoShapeScriptFieldGeoShapeQuery) o;
        return relation == that.relation && Arrays.equals(geometries, that.geometries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relation, Arrays.hashCode(geometries));
    }
}
