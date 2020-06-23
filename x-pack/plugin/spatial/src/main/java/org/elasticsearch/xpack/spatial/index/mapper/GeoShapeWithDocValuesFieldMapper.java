/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;
import org.elasticsearch.xpack.spatial.index.fielddata.AbstractLatLonShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Extension of {@link org.elasticsearch.index.mapper.GeoShapeFieldMapper} that supports docValues
 *
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 */
public class GeoShapeWithDocValuesFieldMapper extends GeoShapeFieldMapper {
    public static final String CONTENT_TYPE = "geo_shape";
    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
    }

    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<Builder, GeoShapeWithDocValuesFieldType> {

        private boolean docValuesSet = false;

        public Builder(String name) {
            super (name, FIELD_TYPE);
            this.hasDocValues = true;
        }

        @Override
        public Builder docValues(boolean docValues) {
            docValuesSet = true;
            return super.docValues(docValues);
        }

        @Override
        public GeoShapeWithDocValuesFieldMapper build(BuilderContext context) {
            if (docValuesSet == false) {
                hasDocValues = Version.V_7_8_0.onOrBefore(context.indexCreatedVersion());
            }
            GeoShapeWithDocValuesFieldType ft = new GeoShapeWithDocValuesFieldType(buildFullName(context), indexed, hasDocValues, meta);
            // @todo check coerce
            GeometryParser geometryParser = new GeometryParser(ft.orientation().getAsBoolean(), coerce().value(),
                ignoreZValue().value());
            ft.setGeometryParser((parser, mapper) -> geometryParser.parse(parser));
            ft.setGeometryIndexer(new GeoShapeIndexer(orientation().value().getAsBoolean(), ft.name()));
            ft.setGeometryQueryBuilder(new VectorGeoShapeQueryProcessor());
            ft.setOrientation(orientation().value());
            return new GeoShapeWithDocValuesFieldMapper(name, fieldType, ft, ignoreMalformed(context), coerce(context),
                ignoreZValue(), orientation(), Version.V_7_8_0.onOrBefore(context.indexCreatedVersion()),
                multiFieldsBuilder.build(this, context), copyTo);
        }

    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void addDocValuesFields(String name, Geometry shape, List fields, ParseContext context) {
        CentroidCalculator calculator = new CentroidCalculator(shape);
        final byte[] scratch = new byte[7 * Integer.BYTES];
        // doc values are generated from the indexed fields.
        ShapeField.DecodedTriangle[] triangles = new ShapeField.DecodedTriangle[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            BytesRef bytesRef = ((List<IndexableField>)fields).get(i).binaryValue();
            assert bytesRef.length == 7 * Integer.BYTES;
            System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
            ShapeField.decodeTriangle(scratch, triangles[i] = new ShapeField.DecodedTriangle());
        }
        BinaryGeoShapeDocValuesField docValuesField =
            (BinaryGeoShapeDocValuesField) context.doc().getByKey(name);
        if (docValuesField == null) {
            docValuesField = new BinaryGeoShapeDocValuesField(name, triangles, calculator);
            context.doc().addWithKey(name, docValuesField);

        } else {
            docValuesField.add(triangles, calculator);
        }
    }

    public static final class GeoShapeWithDocValuesFieldType extends GeoShapeFieldMapper.GeoShapeFieldType {
        public GeoShapeWithDocValuesFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, hasDocValues, meta);
        }

        protected GeoShapeWithDocValuesFieldType(GeoShapeWithDocValuesFieldType ref) {
            super(ref);
        }

        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new AbstractLatLonShapeIndexFieldData.Builder(GeoShapeValuesSourceType.instance());
        }

        @Override
        public GeoShapeWithDocValuesFieldType clone() {
            return new GeoShapeWithDocValuesFieldType(this);
        }
    }

    public static final class TypeParser extends AbstractShapeGeometryFieldMapper.TypeParser {

        @Override
        @SuppressWarnings("rawtypes")
        protected AbstractShapeGeometryFieldMapper.Builder newBuilder(String name, Map<String, Object> params) {
            if (params.containsKey(DEPRECATED_PARAMETERS_KEY)) {
                return new LegacyGeoShapeFieldMapper.Builder(name,
                    (LegacyGeoShapeFieldMapper.DeprecatedParameters)params.get(DEPRECATED_PARAMETERS_KEY));
            }
            return new GeoShapeWithDocValuesFieldMapper.Builder(name);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public AbstractShapeGeometryFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            AbstractShapeGeometryFieldMapper.Builder builder = super.parse(name, node, parserContext);
            Map<String, Object> params = new HashMap<>();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (TypeParsers.DOC_VALUES.equals(fieldName)) {
                    params.put(TypeParsers.DOC_VALUES, XContentMapValues.nodeBooleanValue(fieldNode, name + "." + TypeParsers.DOC_VALUES));
                    iterator.remove();
                }
            }

            if (params.containsKey(TypeParsers.DOC_VALUES)) {
                builder.docValues((Boolean) params.get(TypeParsers.DOC_VALUES));
            }
            return builder;
        }
    }

    private final boolean defaultDocValues;

    public GeoShapeWithDocValuesFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                            Explicit<Boolean> ignoreZValue, Explicit<ShapeBuilder.Orientation> orientation,
                                            boolean defaultDocValues, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation, multiFields, copyTo);
        this.defaultDocValues = defaultDocValues;
    }

    @Override
    protected boolean docValuesByDefault() {
        return defaultDocValues;
    }

    @Override
    public GeoShapeWithDocValuesFieldType fieldType() {
        return (GeoShapeWithDocValuesFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
