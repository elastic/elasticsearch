/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper.ParsedCartesianPoint;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryPointProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Field Mapper for point type.
 *
 * Uses lucene 8 XYPoint encoding
 */
public class PointFieldMapper extends AbstractPointGeometryFieldMapper<List<ParsedCartesianPoint>, List<? extends CartesianPoint>> {
    public static final String CONTENT_TYPE = "point";

    public static class CartesianPointParser extends PointParser<ParsedCartesianPoint> {

        public CartesianPointParser(String name, ParsedPoint nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            super(name, ParsedCartesianPoint::new, (parser, point) -> {
                ParsedCartesianPoint.parsePoint(parser, point, ignoreZValue);
                return point;
            }, (ParsedCartesianPoint) nullValue, ignoreZValue, ignoreMalformed);
        }
    }

    private static Builder builder(FieldMapper in) {
        return ((PointFieldMapper)in).builder;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<ParsedPoint> nullValue;
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.nullValue = nullValueParam(
                m -> builder(m).nullValue.get(),
                (n, c, o) -> o == null ? null : parseNullValue(o, ignoreZValue.get().value(), ignoreMalformed.get().value()),
                () -> null).acceptsNull();
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(indexed, hasDocValues, stored, ignoreMalformed, ignoreZValue, nullValue, meta);
        }

        private static ParsedPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            ParsedCartesianPoint point = new ParsedCartesianPoint();
            CartesianPoint.parsePoint(nullValue, point, ignoreZValue);
            if (ignoreMalformed == false) {
                if (Double.isFinite(point.getX()) == false) {
                    throw new IllegalArgumentException("illegal x value [" + point.getX() + "]");
                }
                if (Double.isFinite(point.getY()) == false) {
                    throw new IllegalArgumentException("illegal y value [" + point.getY() + "]");
                }
            }
            return point;
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            CartesianPointParser parser
                = new CartesianPointParser(name, nullValue.get(), ignoreZValue.get().value(), ignoreMalformed.get().value());
            PointFieldType ft
                = new PointFieldType(buildFullName(context), indexed.get(), stored.get(), hasDocValues.get(), parser, meta.get());
            return new PointFieldMapper(name, ft, multiFieldsBuilder.build(this, context),
                copyTo.build(), new PointIndexer(ft), parser, this);
        }

    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())));

    private final Builder builder;

    public PointFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                            MultiFields multiFields, CopyTo copyTo,
                            PointIndexer pointIndexer, CartesianPointParser parser, Builder builder) {
        super(simpleName, mappedFieldType, multiFields,
            builder.ignoreMalformed.get(), builder.ignoreZValue.get(), builder.nullValue.get(), copyTo, pointIndexer, parser);
        this.builder = builder;
    }

    @Override
    protected void addStoredFields(ParseContext context, List<? extends CartesianPoint> points) {
        for (CartesianPoint point : points) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
    }

    @Override
    protected void addDocValuesFields(String name, List<? extends CartesianPoint> points, List<IndexableField> fields,
                                      ParseContext context) {
        for (CartesianPoint point : points) {
            context.doc().add(new XYDocValuesField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        }
    }

    @Override
    protected void addMultiFields(ParseContext context, List<? extends CartesianPoint> points) {
        // noop
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public PointFieldType fieldType() {
        return (PointFieldType) mappedFieldType;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), builder.ignoreMalformed.getDefaultValue().value()).init(this);
    }

    public static class PointFieldType extends AbstractGeometryFieldType implements ShapeQueryable {

        private final ShapeQueryPointProcessor queryProcessor;

        private PointFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                               CartesianPointParser parser, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, true, parser, meta);
            this.queryProcessor = new ShapeQueryPointProcessor();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            return queryProcessor.shapeQuery(shape, fieldName, relation, context);
        }
    }

    // Eclipse requires the AbstractPointGeometryFieldMapper prefix or it can't find ParsedPoint
    // See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565255
    protected static class ParsedCartesianPoint extends CartesianPoint implements AbstractPointGeometryFieldMapper.ParsedPoint {
        @Override
        public void validate(String fieldName) {
            if (Double.isFinite(getX()) == false) {
                throw new IllegalArgumentException("illegal x value [" + getX() + "] for " + fieldName);
            }
            if (Double.isFinite(getY()) == false) {
                throw new IllegalArgumentException("illegal y value [" + getY() + "] for " + fieldName);
            }
        }

        @Override
        public void normalize(String fieldName) {
            // noop
        }

        @Override
        public boolean isNormalizable(double coord) {
            return false;
        }

        @Override
        public void resetCoords(double x, double y) {
            this.reset(x, y);
        }

        @Override
        public Point asGeometry() {
            return new Point(getX(), getY());
        }

        @Override
        public boolean equals(Object other) {
            double oX;
            double oY;
            if (other instanceof CartesianPoint) {
                CartesianPoint o = (CartesianPoint)other;
                oX = o.getX();
                oY = o.getY();
            } else {
                return false;
            }
            if (Double.compare(oX, x) != 0) return false;
            if (Double.compare(oY, y) != 0) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    protected static class PointIndexer implements Indexer<List<ParsedCartesianPoint>, List<? extends CartesianPoint>> {
        protected final PointFieldType fieldType;

        PointIndexer(PointFieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public List<? extends CartesianPoint> prepareForIndexing(List<ParsedCartesianPoint> points) {
            if (points == null || points.isEmpty()) {
                return Collections.emptyList();
            }
            return points;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<? extends CartesianPoint>> processedClass() {
            return (Class<List<? extends CartesianPoint>>)(Object)List.class;
        }

        @Override
        public List<IndexableField> indexShape(ParseContext context, List<? extends CartesianPoint> points) {
            ArrayList<IndexableField> fields = new ArrayList<>(1);
            for (CartesianPoint point : points) {
                fields.add(new XYPointField(fieldType.name(), (float) point.getX(), (float) point.getY()));
            }
            return fields;
        }
    }
}
