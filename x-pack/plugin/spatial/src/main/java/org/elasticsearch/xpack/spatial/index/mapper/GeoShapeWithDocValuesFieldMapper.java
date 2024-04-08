/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
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
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.mapper.StoredValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.LatLonShapeDocValuesQuery;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.AbstractScriptFieldFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractAtomicGeoShapeShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.LatLonShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
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

    public static final class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);
        final Parameter<Boolean> hasDocValues;

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        final Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());
        private final Parameter<Script> script = Parameter.scriptParam(m -> builder(m).script.get());
        private final Parameter<OnScriptError> onScriptError = Parameter.onScriptErrorParam(m -> builder(m).onScriptError.get(), script);
        final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final ScriptCompiler scriptCompiler;
        private final IndexVersion version;
        private final GeoFormatterFactory<Geometry> geoFormatterFactory;

        public Builder(
            String name,
            IndexVersion version,
            ScriptCompiler scriptCompiler,
            boolean ignoreMalformedByDefault,
            boolean coerceByDefault,
            GeoFormatterFactory<Geometry> geoFormatterFactory
        ) {
            super(name);
            this.version = version;
            this.scriptCompiler = scriptCompiler;
            this.geoFormatterFactory = geoFormatterFactory;
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);
            this.hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), IndexVersions.V_7_8_0.onOrBefore(version));
            addScriptValidation(script, indexed, hasDocValues);
        }

        // for testing
        Builder setStored(boolean stored) {
            this.stored.setValue(stored);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                indexed,
                hasDocValues,
                stored,
                ignoreMalformed,
                ignoreZValue,
                coerce,
                orientation,
                script,
                onScriptError,
                meta };
        }

        private FieldValues<Geometry> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            GeometryFieldScript.Factory factory = scriptCompiler.compile(this.script.get(), GeometryFieldScript.CONTEXT);
            return factory == null
                ? null
                : (lookup, ctx, doc, consumer) -> factory.newFactory(name(), script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
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
                context.buildFullName(name()),
                indexed.get(),
                hasDocValues.get(),
                stored.get(),
                orientation.get().value(),
                parser,
                scriptValues(),
                geoFormatterFactory,
                meta.get()
            );
            if (script.get() == null) {
                return new GeoShapeWithDocValuesFieldMapper(
                    name(),
                    ft,
                    multiFieldsBuilder.build(this, context),
                    copyTo,
                    new GeoShapeIndexer(orientation.get().value(), ft.name()),
                    parser,
                    this
                );
            }
            return new GeoShapeWithDocValuesFieldMapper(
                name(),
                ft,
                multiFieldsBuilder.build(this, context),
                copyTo,
                new GeoShapeIndexer(orientation.get().value(), ft.name()),
                parser,
                onScriptError.get(),
                this
            );
        }

    }

    public static final class GeoShapeWithDocValuesFieldType extends AbstractShapeGeometryFieldType<Geometry> implements GeoShapeQueryable {

        private final GeoFormatterFactory<Geometry> geoFormatterFactory;
        private final FieldValues<Geometry> scriptValues;

        public GeoShapeWithDocValuesFieldType(
            String name,
            boolean indexed,
            boolean hasDocValues,
            boolean isStored,
            Orientation orientation,
            GeoShapeParser parser,
            FieldValues<Geometry> scriptValues,
            GeoFormatterFactory<Geometry> geoFormatterFactory,
            Map<String, String> meta
        ) {
            super(name, indexed, isStored, hasDocValues, parser, orientation, meta);
            this.scriptValues = scriptValues;
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
            failIfNotIndexedNorDocValuesFallback(context);
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(IndexVersions.V_7_5_0)) {
                throw new QueryShardException(
                    context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]."
                );
            }
            Query query;
            if (isIndexed()) {
                query = LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), geometries);
                if (hasDocValues()) {
                    final Query queryDocValues = new LatLonShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), geometries);
                    query = new IndexOrDocValuesQuery(query, queryDocValues);
                }
            } else {
                query = new LatLonShapeDocValuesQuery(fieldName, relation.getLuceneRelation(), geometries);
            }
            return query;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (isStored()) {
                Function<List<Geometry>, List<Object>> formatter = getFormatter(format != null ? format : GeometryFormatterFactory.GEOJSON);
                return new StoredValueFetcher(context.lookup(), name()) {
                    @Override
                    public List<Object> parseStoredValues(List<Object> storedValues) {
                        final List<Geometry> values = new ArrayList<>(storedValues.size());
                        for (Object storedValue : storedValues) {
                            if (storedValue instanceof BytesRef bytesRef) {
                                values.add(
                                    WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, bytesRef.bytes, bytesRef.offset, bytesRef.length)
                                );
                            } else {
                                throw new IllegalArgumentException("Unexpected class fetching [" + name() + "]: " + storedValue.getClass());
                            }
                        }
                        return formatter.apply(values);
                    }
                };
            }
            if (scriptValues != null) {
                Function<List<Geometry>, List<Object>> formatter = getFormatter(format != null ? format : GeometryFormatterFactory.GEOJSON);
                return FieldValues.valueListFetcher(scriptValues, formatter, context);
            }
            return super.valueFetcher(context, format);
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
                if (parserContext.indexVersionCreated().onOrAfter(IndexVersions.V_8_0_0)) {
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
                    parserContext.scriptCompiler(),
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

    protected GeoShapeWithDocValuesFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        GeoShapeIndexer indexer,
        Parser<Geometry> parser,
        OnScriptError onScriptError,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, builder.coerce.get(), builder.orientation.get(), copyTo, parser, onScriptError);
        this.builder = builder;
        this.indexer = indexer;
    }

    @Override
    protected void index(DocumentParserContext context, Geometry geometry) {
        // TODO: Make common with the index method ShapeFieldMapper
        if (geometry == null) {
            return;
        }
        final Geometry normalizedGeometry = indexer.normalize(geometry);
        final List<IndexableField> fields = indexer.getIndexableFields(normalizedGeometry);
        if (fieldType().isIndexed()) {
            context.doc().addAll(fields);
        }
        if (fieldType().hasDocValues()) {
            final String name = fieldType().name();
            BinaryShapeDocValuesField docValuesField = (BinaryShapeDocValuesField) context.doc().getByKey(name);
            if (docValuesField == null) {
                docValuesField = new BinaryShapeDocValuesField(name, CoordinateEncoder.GEO);
                context.doc().addWithKey(name, docValuesField);
            }
            // we need to pass the original geometry to compute more precisely the centroid, e.g if lon > 180
            docValuesField.add(fields, geometry);
        } else if (fieldType().isIndexed()) {
            context.addToFieldNames(fieldType().name());
        }

        if (fieldType().isStored()) {
            context.doc().add(new StoredField(fieldType().name(), WellKnownBinary.toWKB(normalizedGeometry, ByteOrder.LITTLE_ENDIAN)));
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        fieldType().scriptValues.valuesForDoc(searchLookup, readerContext, doc, geometry -> index(documentParserContext, geometry));
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
            builder.scriptCompiler,
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

    public static class GeoShapeDocValuesField extends AbstractScriptFieldFactory<GeoShapeValues.GeoShapeValue>
        implements
            Field<GeoShapeValues.GeoShapeValue>,
            DocValuesScriptFieldFactory,
            ScriptDocValues.GeometrySupplier<GeoPoint, GeoShapeValues.GeoShapeValue> {

        private final GeoShapeValues in;
        protected final String name;

        private GeoShapeValues.GeoShapeValue value;

        // maintain bwc by making bounding box and centroid available to GeoShapeValues (ScriptDocValues)
        private final GeoPoint centroid = new GeoPoint();
        private final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        private ScriptDocValues<GeoShapeValues.GeoShapeValue> geoShapeScriptValues;

        public GeoShapeDocValuesField(GeoShapeValues in, String name) {
            this.in = in;
            this.name = name;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                value = in.value();
                centroid.reset(value.getY(), value.getX());
                boundingBox.topLeft().reset(value.boundingBox().maxY(), value.boundingBox().minX());
                boundingBox.bottomRight().reset(value.boundingBox().minY(), value.boundingBox().maxX());
            } else {
                value = null;
            }
        }

        @Override
        public ScriptDocValues<GeoShapeValues.GeoShapeValue> toScriptDocValues() {
            if (geoShapeScriptValues == null) {
                geoShapeScriptValues = new AbstractAtomicGeoShapeShapeFieldData.GeoShapeScriptValues(this);
            }

            return geoShapeScriptValues;
        }

        @Override
        public GeoShapeValues.GeoShapeValue getInternal(int index) {
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
        public Iterator<GeoShapeValues.GeoShapeValue> iterator() {
            return new Iterator<>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < size();
                }

                @Override
                public GeoShapeValues.GeoShapeValue next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return value;
                }
            };
        }
    }
}
