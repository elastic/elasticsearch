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
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper;
import org.elasticsearch.index.mapper.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryPointProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Field Mapper for point type.
 *
 * Uses lucene 8 XYPoint encoding
 */
public class PointFieldMapper extends AbstractPointGeometryFieldMapper<CartesianPoint, CartesianPoint> implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "point";

    public static class Builder extends AbstractPointGeometryFieldMapper.Builder<Builder, PointFieldMapper, PointFieldType> {
        public Builder(String name) {
            super(name, new PointFieldType(), new PointFieldType());
            builder = this;
        }

        @Override
        public PointFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                      MappedFieldType defaultFieldType, Settings indexSettings,
                                      MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                      Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
            setupFieldType(context);
            return new PointFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                ignoreMalformed, ignoreZValue(context), copyTo);
        }

        @Override
        protected void setGeometryParser() {
            PointParser<ParsedCartesianPoint> pointParser = new PointParser<>();
            fieldType().setGeometryParser((parser, mapper) -> pointParser.parse(parser, mapper));
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void setGeometryIndexer(PointFieldType fieldType) {
            fieldType.setGeometryIndexer(new PointIndexer(fieldType));
        }

        @Override
        protected void setGeometryQueryBuilder(PointFieldType fieldType) {
            fieldType.setGeometryQueryBuilder(new ShapeQueryPointProcessor());
        }
    }

    @Override
    protected ParsedPoint newParsedPoint() {
        return new ParsedCartesianPoint();
    }

    public static class TypeParser extends AbstractPointGeometryFieldMapper.TypeParser<CartesianPoint, Builder> {
        @Override
        protected Builder newBuilder(String name, Map<String, Object> params) {
            return new PointFieldMapper.Builder(name);
        }

        @Override
        protected CartesianPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            CartesianPoint point = CartesianPoint.parsePoint(nullValue, ignoreZValue);
            if (ignoreMalformed == false) {
                if (Float.isFinite(point.getX()) == false) {
                    throw new IllegalArgumentException("illegal x value [" + point.getX() + "]");
                }
                if (Float.isFinite(point.getY()) == false) {
                    throw new IllegalArgumentException("illegal y value [" + point.getY() + "]");
                }
            }
            return point;
        }
    }

    /**
     * Parses geopoint represented as an object or an array, ignores malformed geopoints if needed
     */
    @Override
    protected void parsePointIgnoringMalformed(XContentParser parser, ParsedPoint point) throws IOException {
        super.parsePointIgnoringMalformed(parser, point);
        CartesianPoint.parsePoint(parser, (CartesianPoint)point, ignoreMalformed().value());
    }

    public PointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                            Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                            Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, ignoreMalformed, ignoreZValue, copyTo);
    }

    @Override
    protected void addStoredFields(ParseContext context, CartesianPoint point) {
        context.doc().add(new StoredField(fieldType().name(), point.toString()));
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void addDocValuesFields(String name, CartesianPoint point, List fields, ParseContext context) {
        context.doc().add(new XYDocValuesField(fieldType().name(), point.getX(), point.getY()));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public PointFieldType fieldType() {
        return (PointFieldType)fieldType;
    }

    public static class PointFieldType extends AbstractPointGeometryFieldType<CartesianPoint, CartesianPoint> {
        public PointFieldType() {
            super();
        }

         PointFieldType(PointFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new PointFieldType(this);
        }
    }

    protected static class ParsedCartesianPoint extends CartesianPoint implements ParsedPoint {
        @Override
        public void validate(String fieldName) {
            if (Float.isFinite(getX()) == false) {
                throw new IllegalArgumentException("illegal x value [" + getX() + "] for " + fieldName);
            }
            if (Float.isFinite(getY()) == false) {
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
            this.reset((float)x, (float)y);
        }
    }

    protected static class PointIndexer implements Indexer<CartesianPoint, CartesianPoint> {
        protected final PointFieldType fieldType;

        PointIndexer(PointFieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public CartesianPoint prepareForIndexing(CartesianPoint point) {
            if (point == null) {
                return null;
            }
            return point;
        }

        @Override
        public Class<CartesianPoint> processedClass() {
            return CartesianPoint.class;
        }

        @Override
        public List<IndexableField> indexShape(ParseContext context, CartesianPoint point) {
            ArrayList<IndexableField> fields = new ArrayList<>(1);
            fields.add(new XYPointField(fieldType.name(), point.getX(), point.getY()));
            return fields;
        }
    }
}
