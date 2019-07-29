/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * FieldMapper for indexing cartesian {@link XYShape}s.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [1050.0, -1000.0], [1051.0, -1000.0], [1051.0, -1001.0], [1050.0, -1001.0], [1050.0, -1000.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((1050.0 -1000.0, 1051.0 -1000.0, 1051.0 -1001.0, 1050.0 -1001.0, 1050.0 -1000.0))
 */
public class ShapeFieldMapper extends AbstractGeometryFieldMapper<Geometry, Geometry> {
    public static final String CONTENT_TYPE = "shape";

    public static class Defaults extends AbstractGeometryFieldMapper.Defaults {
        public static final ShapeFieldType FIELD_TYPE = new ShapeFieldType();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static class Builder extends AbstractGeometryFieldMapper.Builder<AbstractGeometryFieldMapper.Builder, ShapeFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public ShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new ShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        public ShapeFieldType fieldType() {
            return (ShapeFieldType)fieldType;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeometryParser geometryParser = new GeometryParser(orientation == ShapeBuilder.Orientation.RIGHT,
                coerce(context).value(), ignoreZValue().value());

            fieldType().setGeometryIndexer(new ShapeIndexer());
            fieldType().setGeometryParser((parser, mapper) -> geometryParser.parse(parser));
            fieldType().setGeometryQueryBuilder(new ShapeQueryProcessor());
        }
    }

    public static class TypeParser extends AbstractGeometryFieldMapper.TypeParser {
        @Override
        protected boolean parseXContentParameters(String name, Map.Entry<String, Object> entry,
                                                  Map<String, Object> params) throws MapperParsingException {
            return false;
        }

        @Override
        public Builder newBuilder(String name, Map<String, Object> params) {
            return new Builder(name);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final class ShapeFieldType extends AbstractGeometryFieldType {
        public ShapeFieldType() {
            super();
        }

        public ShapeFieldType(ShapeFieldType ref) {
            super(ref);
        }

        @Override
        public ShapeFieldType clone() {
            return new ShapeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected Indexer<Geometry, Geometry> geometryIndexer() {
            return geometryIndexer;
        }
    }

    public ShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                            Explicit<Boolean> ignoreZValue, Settings indexSettings,
                            MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ShapeFieldType fieldType() {
        return (ShapeFieldType) super.fieldType();
    }

    @Override
    protected void indexShape(ParseContext context, Geometry shape) {
        shape.visit(new LuceneGeometryIndexer(context));
    }

    /** @todo move to {@link ShapeIndexer} after refactor is complete */
    private class LuceneGeometryIndexer implements GeometryVisitor<Void, RuntimeException> {
        private ParseContext context;

        private LuceneGeometryIndexer(ParseContext context) {
            this.context = context;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while indexing shape");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Line line) {
            float[] x = new float[line.length()];
            float[] y = new float[x.length];
            for (int i = 0; i < x.length; ++i) {
                x[i] = (float)line.getLon(i);
                y[i] = (float)line.getLat(i);
            }
            indexFields(context, XYShape.createIndexableFields(name(), new XYLine(x, y)));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (org.elasticsearch.geo.geometry.Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for(Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for(org.elasticsearch.geo.geometry.Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            indexFields(context, XYShape.createIndexableFields(name(), (float)point.getLon(), (float)point.getLat()));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Polygon polygon) {
            indexFields(context, XYShape.createIndexableFields(name(), toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Rectangle r) {
            XYPolygon p = new XYPolygon(
                new float[]{(float)r.getMinLon(), (float)r.getMaxLon(), (float)r.getMaxLon(), (float)r.getMinLon(), (float)r.getMinLon()},
                new float[]{(float)r.getMinLat(), (float)r.getMinLat(), (float)r.getMaxLat(), (float)r.getMaxLat(), (float)r.getMinLat()});
            indexFields(context, XYShape.createIndexableFields(name(), p));
            return null;
        }

        private void indexFields(ParseContext context, Field[] fields) {
            ArrayList<IndexableField> flist = new ArrayList<>(Arrays.asList(fields));
            createFieldNamesField(context, flist);
            for (IndexableField f : flist) {
                context.doc().add(f);
            }
        }
    }

    public static XYPolygon toLucenePolygon(org.elasticsearch.geo.geometry.Polygon polygon) {
        XYPolygon[] holes = new XYPolygon[polygon.getNumberOfHoles()];
        LinearRing ring;
        for(int i = 0; i<holes.length; i++) {
            ring = polygon.getHole(i);
            float[] x = new float[ring.length()];
            float[] y = new float[x.length];
            for (int j = 0; j < x.length; ++j) {
                x[j] = (float)ring.getLon(j);
                y[j] = (float)ring.getLat(j);
            }
            holes[i] = new XYPolygon(x, y);
        }
        ring = polygon.getPolygon();
        float[] x = new float[ring.length()];
        float[] y = new float[x.length];
        for (int i = 0; i < x.length; ++i) {
            x[i] = (float)ring.getLon(i);
            y[i] = (float)ring.getLat(i);
        }
        return new XYPolygon(x, y, holes);
    }
}
