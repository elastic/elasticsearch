/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.GeoShapeParser;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.script.field.AbstractScriptFieldFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractAtomicGeoShapeShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.LatLonShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

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
public class GeoShapeWithDocValuesFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry> {
    public static final String CONTENT_TYPE = "geo_shape";

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(GeoShapeFieldMapper.class);

    private static Builder builder(FieldMapper in) {
        return ((GeoShapeWithDocValuesFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues;

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        final Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Version version;
        private final GeoFormatterFactory<Geometry> geoFormatterFactory;

        public Builder(
            String name,
            Version version,
            boolean ignoreMalformedByDefault,
            boolean coerceByDefault,
            GeoFormatterFactory<Geometry> geoFormatterFactory
        ) {
            super(name);
            this.version = version;
            this.geoFormatterFactory = geoFormatterFactory;
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);
            this.hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), Version.V_7_8_0.onOrBefore(version));
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { indexed, hasDocValues, ignoreMalformed, ignoreZValue, coerce, orientation, meta };
        }

        @Override
        public GeoShapeWithDocValuesFieldMapper build(MapperBuilderContext context) {
            if (multiFieldsBuilder.hasMultiFields()) {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.MAPPINGS,
                    "geo_shape_multifields",
                    "Adding multifields to [geo_shape] mappers has no effect and will be forbidden in future"
                );
            }
            GeometryParser geometryParser = new GeometryParser(
                orientation.get().value().getAsBoolean(),
                coerce.get().value(),
                ignoreZValue.get().value()
            );
            GeoShapeParser parser = new GeoShapeParser(geometryParser, orientation.get().value());
            GeoShapeWithDocValuesFieldType ft = new GeoShapeWithDocValuesFieldType(
                context.buildFullName(name),
                indexed.get(),
                hasDocValues.get(),
                orientation.get().value(),
                parser,
                geoFormatterFactory,
                meta.get()
            );
            return new GeoShapeWithDocValuesFieldMapper(
                name,
                ft,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                new GeoShapeIndexer(orientation.get().value(), ft.name()),
                parser,
                this
            );
        }

    }

    public static final class GeoShapeWithDocValuesFieldType extends AbstractShapeGeometryFieldType<Geometry> implements GeoShapeQueryable {

        private final GeoFormatterFactory<Geometry> geoFormatterFactory;

        public GeoShapeWithDocValuesFieldType(
            String name,
            boolean indexed,
            boolean hasDocValues,
            Orientation orientation,
            GeoShapeParser parser,
            GeoFormatterFactory<Geometry> geoFormatterFactory,
            Map<String, String> meta
        ) {
            super(name, indexed, false, hasDocValues, parser, orientation, meta);
            this.geoFormatterFactory = geoFormatterFactory;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return (cache, breakerService) -> new LatLonShapeIndexFieldData(
                name(),
                GeoShapeValuesSourceType.instance(),
                GeoShapeDocValuesField::new
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, LatLonGeometry... geometries) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
                throw new QueryShardException(
                    context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]."
                );
            }
            Query query = LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), geometries);
            if (hasDocValues()) {
                final Query queryDocValues = new LatLonShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), geometries);
                query = new IndexOrDocValuesQuery(query, queryDocValues);
            }
            return query;
        }

        @Override
        protected Function<List<Geometry>, List<Object>> getFormatter(String format) {
            return geoFormatterFactory.getFormatter(format, Function.identity());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private final GeoFormatterFactory<Geometry> geoFormatterFactory;

        public TypeParser(GeoFormatterFactory<Geometry> geoFormatterFactory) {
            this.geoFormatterFactory = geoFormatterFactory;
        }

        @Override
        @SuppressWarnings("deprecation")
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            FieldMapper.Builder builder;
            boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(parserContext.getSettings());
            boolean coerceByDefault = COERCE_SETTING.get(parserContext.getSettings());
            if (LegacyGeoShapeFieldMapper.containsDeprecatedParameter(node.keySet())) {
                if (parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0)) {
                    Set<String> deprecatedParams = LegacyGeoShapeFieldMapper.getDeprecatedParameters(node.keySet());
                    throw new IllegalArgumentException(
                        "using deprecated parameters "
                            + Arrays.toString(deprecatedParams.toArray())
                            + " in mapper ["
                            + name
                            + "] of type [geo_shape] is no longer allowed"
                    );
                }
                builder = new LegacyGeoShapeFieldMapper.Builder(
                    name,
                    parserContext.indexVersionCreated(),
                    ignoreMalformedByDefault,
                    coerceByDefault
                );
            } else {
                builder = new GeoShapeWithDocValuesFieldMapper.Builder(
                    name,
                    parserContext.indexVersionCreated(),
                    ignoreMalformedByDefault,
                    coerceByDefault,
                    geoFormatterFactory
                );
            }
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

    private final Builder builder;
    private final GeoShapeIndexer indexer;

    public GeoShapeWithDocValuesFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        GeoShapeIndexer indexer,
        GeoShapeParser parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            builder.ignoreMalformed.get(),
            builder.coerce.get(),
            builder.ignoreZValue.get(),
            builder.orientation.get(),
            multiFields,
            copyTo,
            parser
        );
        this.builder = builder;
        this.indexer = indexer;
    }

    @Override
    protected void index(DocumentParserContext context, Geometry geometry) throws IOException {
        if (geometry == null) {
            return;
        }
        List<IndexableField> fields = indexer.indexShape(geometry);
        if (fieldType().isIndexed()) {
            context.doc().addAll(fields);
        }
        if (fieldType().hasDocValues()) {
            String name = fieldType().name();
            BinaryShapeDocValuesField docValuesField = (BinaryShapeDocValuesField) context.doc().getByKey(name);
            if (docValuesField == null) {
                docValuesField = new BinaryShapeDocValuesField(name, CoordinateEncoder.GEO);
                context.doc().addWithKey(name, docValuesField);
            }
            docValuesField.add(fields, geometry);
        } else if (fieldType().isIndexed()) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            simpleName(),
            builder.version,
            builder.ignoreMalformed.getDefaultValue().value(),
            builder.coerce.getDefaultValue().value(),
            builder.geoFormatterFactory
        ).init(this);
    }

    @Override
    public GeoShapeWithDocValuesFieldType fieldType() {
        return (GeoShapeWithDocValuesFieldType) super.fieldType();
    }

    @Override
    protected void checkIncomingMergeType(FieldMapper mergeWith) {
        if (mergeWith instanceof GeoShapeWithDocValuesFieldMapper == false && CONTENT_TYPE.equals(mergeWith.typeName())) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] of type [geo_shape] cannot change strategy from [BKD] to [recursive]"
            );
        }
        super.checkIncomingMergeType(mergeWith);
    }

    public static class GeoShapeDocValuesField extends AbstractScriptFieldFactory<ShapeValues.ShapeValue>
        implements
            Field<ShapeValues.ShapeValue>,
            DocValuesScriptFieldFactory,
            ScriptDocValues.GeometrySupplier<GeoPoint, ShapeValues.ShapeValue> {

        private final ShapeValues in;
        protected final String name;

        private GeoShapeValues.GeoShapeValue value;

        // maintain bwc by making bounding box and centroid available to GeoShapeValues (ScriptDocValues)
        private final GeoPoint centroid = new GeoPoint();
        private final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        private LeafShapeFieldData.ShapeScriptValues<GeoPoint> geoShapeScriptValues;

        public GeoShapeDocValuesField(ShapeValues in, String name) {
            this.in = in;
            this.name = name;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                value = (GeoShapeValues.GeoShapeValue) in.value();
                centroid.reset(value.getY(), value.getX());
                boundingBox.topLeft().reset(value.boundingBox().maxY(), value.boundingBox().minX());
                boundingBox.bottomRight().reset(value.boundingBox().minY(), value.boundingBox().maxX());
            } else {
                value = null;
            }
        }

        @Override
        public ScriptDocValues<ShapeValues.ShapeValue> toScriptDocValues() {
            if (geoShapeScriptValues == null) {
                geoShapeScriptValues = new AbstractAtomicGeoShapeShapeFieldData.GeoShapeScriptValues(this);
            }

            return geoShapeScriptValues;
        }

        @Override
        public ShapeValues.ShapeValue getInternal(int index) {
            if (index != 0) {
                throw new UnsupportedOperationException();
            }

            return value;
        }

        // maintain bwc by making centroid available to GeoShapeValues (ScriptDocValues)
        @Override
        public GeoPoint getInternalCentroid() {
            return centroid;
        }

        // maintain bwc by making centroid available to GeoShapeValues (ScriptDocValues)
        @Override
        public GeoBoundingBox getInternalBoundingBox() {
            return boundingBox;
        }

        @Override
        public GeoPoint getInternalLabelPosition() {
            try {
                return new GeoPoint(value.labelPosition());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to parse geo shape label position: " + e.getMessage(), e);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isEmpty() {
            return value == null;
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }

        public GeoShapeValues.GeoShapeValue get(GeoShapeValues.GeoShapeValue defaultValue) {
            return get(0, defaultValue);
        }

        public GeoShapeValues.GeoShapeValue get(int index, GeoShapeValues.GeoShapeValue defaultValue) {
            if (isEmpty() || index != 0) {
                return defaultValue;
            }

            return value;
        }

        @Override
        public Iterator<ShapeValues.ShapeValue> iterator() {
            return new Iterator<>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < size();
                }

                @Override
                public ShapeValues.ShapeValue next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return value;
                }
            };
        }
    }
}
