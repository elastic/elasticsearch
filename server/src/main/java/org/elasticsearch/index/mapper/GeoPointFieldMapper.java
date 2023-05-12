/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SimpleVectorTileFormatter;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherMultiGeoPointIndexFieldData;
import org.elasticsearch.index.fielddata.plain.LatLonPointIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.SortedNumericDocValuesLongFieldScript;
import org.elasticsearch.script.field.GeoPointDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends AbstractPointGeometryFieldMapper<GeoPoint> {

    public static final String CONTENT_TYPE = "geo_point";

    private static Builder builder(FieldMapper in) {
        return toType(in).builder;
    }

    private static GeoPointFieldMapper toType(FieldMapper in) {
        return (GeoPointFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<GeoPoint> nullValue;
        final Parameter<Boolean> indexed;
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);
        private final Parameter<Script> script = Parameter.scriptParam(m -> builder(m).script.get());
        private final Parameter<OnScriptError> onScriptError = Parameter.onScriptErrorParam(m -> builder(m).onScriptError.get(), script);
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final ScriptCompiler scriptCompiler;
        private final IndexVersion indexCreatedVersion;
        private final Parameter<TimeSeriesParams.MetricType> metric;  // either null, or POSITION if this is a time series metric
        private final Parameter<Boolean> dimension; // can only support time_series_dimension: false
        private final IndexMode indexMode;  // either STANDARD or TIME_SERIES

        public Builder(
            String name,
            ScriptCompiler scriptCompiler,
            boolean ignoreMalformedByDefault,
            IndexVersion indexCreatedVersion,
            IndexMode mode
        ) {
            super(name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.nullValue = nullValueParam(
                m -> builder(m).nullValue.get(),
                (n, c, o) -> parseNullValue(o, ignoreZValue.get().value(), ignoreMalformed.get().value()),
                () -> null,
                XContentBuilder::field
            ).acceptsNull();
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.indexCreatedVersion = Objects.requireNonNull(indexCreatedVersion);
            this.script.precludesParameters(nullValue, ignoreMalformed, ignoreZValue);
            this.indexMode = mode;
            this.indexed = Parameter.indexParam(
                m -> toType(m).indexed,
                () -> indexMode != IndexMode.TIME_SERIES || getMetric().getValue() != TimeSeriesParams.MetricType.POSITION
            );
            addScriptValidation(script, indexed, hasDocValues);

            this.metric = TimeSeriesParams.metricParam(m -> toType(m).metricType, TimeSeriesParams.MetricType.POSITION).addValidator(v -> {
                if (v != null && hasDocValues.getValue() == false) {
                    throw new IllegalArgumentException(
                        "Field [" + TimeSeriesParams.TIME_SERIES_METRIC_PARAM + "] requires that [" + hasDocValues.name + "] is true"
                    );
                }
            });
            // We allow `time_series_dimension` parameter to be parsed, but only allow it to be `false`
            this.dimension = TimeSeriesParams.dimensionParam(m -> false).addValidator(v -> {
                if (v) {
                    throw new IllegalArgumentException(
                        "Parameter [" + TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + "] cannot be set to geo_point"
                    );
                }
            });
        }

        private Parameter<TimeSeriesParams.MetricType> getMetric() {
            return metric;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                hasDocValues,
                indexed,
                stored,
                ignoreMalformed,
                ignoreZValue,
                nullValue,
                script,
                onScriptError,
                meta,
                dimension,
                metric };
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        private static GeoPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            if (nullValue == null) {
                return null;
            }
            GeoPoint point = GeoUtils.parseGeoPoint(nullValue, ignoreZValue);
            if (ignoreMalformed == false) {
                if (point.lat() > 90.0 || point.lat() < -90.0) {
                    throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "]");
                }
                if (point.lon() > 180.0 || point.lon() < -180) {
                    throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "]");
                }
            } else {
                GeoUtils.normalizePoint(point);
            }
            return point;
        }

        private FieldValues<GeoPoint> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            GeoPointFieldScript.Factory factory = scriptCompiler.compile(this.script.get(), GeoPointFieldScript.CONTEXT);
            return factory == null
                ? null
                : (lookup, ctx, doc, consumer) -> factory.newFactory(name, script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            Parser<GeoPoint> geoParser = new GeoPointParser(
                name,
                (parser) -> GeoUtils.parseGeoPoint(parser, ignoreZValue.get().value()),
                nullValue.get(),
                ignoreZValue.get().value(),
                ignoreMalformed.get().value(),
                metric.get() != TimeSeriesParams.MetricType.POSITION
            );
            GeoPointFieldType ft = new GeoPointFieldType(
                context.buildFullName(name),
                indexed.get() && indexCreatedVersion.isLegacyIndexVersion() == false,
                stored.get(),
                hasDocValues.get(),
                geoParser,
                scriptValues(),
                meta.get(),
                metric.get()
            );
            if (this.script.get() == null) {
                return new GeoPointFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), geoParser, this);
            }
            return new GeoPointFieldMapper(name, ft, geoParser, this);
        }

    }

    private static final IndexVersion MINIMUM_COMPATIBILITY_VERSION = IndexVersion.fromId(5000099);

    public static TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(
            n,
            c.scriptCompiler(),
            IGNORE_MALFORMED_SETTING.get(c.getSettings()),
            c.indexVersionCreated(),
            c.getIndexSettings().getMode()
        ),
        MINIMUM_COMPATIBILITY_VERSION
    );

    private final Builder builder;
    private final FieldValues<GeoPoint> scriptValues;
    private final IndexVersion indexCreatedVersion;
    private final TimeSeriesParams.MetricType metricType;
    private final IndexMode indexMode;
    private final boolean indexed;

    public GeoPointFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Parser<GeoPoint> parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            multiFields,
            builder.ignoreMalformed.get(),
            builder.ignoreZValue.get(),
            builder.nullValue.get(),
            copyTo,
            parser
        );
        this.builder = builder;
        this.scriptValues = null;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.metricType = builder.metric.get();
        this.indexMode = builder.indexMode;
        this.indexed = builder.indexed.get();
    }

    public GeoPointFieldMapper(String simpleName, MappedFieldType mappedFieldType, Parser<GeoPoint> parser, Builder builder) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty(), parser, builder.onScriptError.get());
        this.builder = builder;
        this.scriptValues = builder.scriptValues();
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.metricType = builder.metric.get();
        this.indexMode = builder.indexMode;
        this.indexed = builder.indexed.get();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            simpleName(),
            builder.scriptCompiler,
            builder.ignoreMalformed.getDefaultValue().value(),
            indexCreatedVersion,
            indexMode
        ).init(this);
    }

    @Override
    protected void index(DocumentParserContext context, GeoPoint geometry) throws IOException {
        if (fieldType().isIndexed()) {
            context.doc().add(new LatLonPoint(fieldType().name(), geometry.lat(), geometry.lon()));
        }
        if (fieldType().hasDocValues()) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), geometry.lat(), geometry.lon()));
        } else if (fieldType().isStored() || fieldType().isIndexed()) {
            context.addToFieldNames(fieldType().name());
        }
        if (fieldType().isStored()) {
            context.doc().add(new StoredField(fieldType().name(), geometry.toString()));
        }
        // TODO phase out geohash (which is currently used in the CompletionSuggester)
        // we only expose the geohash value and disallow advancing tokens, hence we can reuse the same parser throughout multiple sub-fields
        DocumentParserContext parserContext = context.switchParser(new GeoHashMultiFieldParser(context.parser(), geometry.geohash()));
        multiFields.parse(this, context, () -> parserContext);
    }

    /**
     * Parser that pretends to be the main document parser, but exposes the provided geohash regardless of how the geopoint was provided
     * in the incoming document. We rely on the fact that consumers are only ever call {@link XContentParser#textOrNull()} and never
     * advance tokens, which is explicitly disallowed by this parser.
     */
    static class GeoHashMultiFieldParser extends FilterXContentParserWrapper {
        private final String value;

        GeoHashMultiFieldParser(XContentParser innerParser, String value) {
            super(innerParser);
            this.value = value;
        }

        @Override
        public String textOrNull() throws IOException {
            return value;
        }

        @Override
        public Token currentToken() {
            return Token.VALUE_STRING;
        }

        @Override
        public Token nextToken() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, point -> {
            try {
                index(documentParserContext, point);
            } catch (IOException e) {
                throw new UncheckedIOException(e);  // only thrown by MultiFields which is always null
            }
        });
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class GeoPointFieldType extends AbstractGeometryFieldType<GeoPoint> implements GeoShapeQueryable {
        private final TimeSeriesParams.MetricType metricType;

        public static final GeoFormatterFactory<GeoPoint> GEO_FORMATTER_FACTORY = new GeoFormatterFactory<>(
            List.of(new SimpleVectorTileFormatter())
        );

        private final FieldValues<GeoPoint> scriptValues;

        private GeoPointFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Parser<GeoPoint> parser,
            FieldValues<GeoPoint> scriptValues,
            Map<String, String> meta,
            TimeSeriesParams.MetricType metricType
        ) {
            super(name, indexed, stored, hasDocValues, parser, meta);
            this.scriptValues = scriptValues;
            this.metricType = metricType;
        }

        // only used in test
        public GeoPointFieldType(String name) {
            this(name, true, false, true, null, null, Collections.emptyMap(), null);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return isIndexed() || hasDocValues();
        }

        @Override
        protected Function<List<GeoPoint>, List<Object>> getFormatter(String format) {
            return GEO_FORMATTER_FACTORY.getFormatter(format, p -> new Point(p.getLon(), p.getLat()));
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (scriptValues == null) {
                return super.valueFetcher(context, format);
            }
            Function<List<GeoPoint>, List<Object>> formatter = getFormatter(format != null ? format : GeometryFormatterFactory.GEOJSON);
            return FieldValues.valueListFetcher(scriptValues, formatter, context);
        }

        @Override
        public Query geoShapeQuery(SearchExecutionContext context, String fieldName, ShapeRelation relation, LatLonGeometry... geometries) {
            failIfNotIndexedNorDocValuesFallback(context);
            final ShapeField.QueryRelation luceneRelation;
            if (relation == ShapeRelation.INTERSECTS && isPointGeometry(geometries)) {
                // For point queries and intersects, lucene does not match points that are encoded
                // to Integer.MAX_VALUE because the use of ComponentPredicate for speeding up queries.
                // We use contains instead.
                luceneRelation = ShapeField.QueryRelation.CONTAINS;
            } else {
                luceneRelation = relation.getLuceneRelation();
            }
            Query query;
            if (isIndexed()) {
                query = LatLonPoint.newGeometryQuery(fieldName, luceneRelation, geometries);
                if (hasDocValues()) {
                    Query dvQuery = LatLonDocValuesField.newSlowGeometryQuery(fieldName, luceneRelation, geometries);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
            } else {
                query = LatLonDocValuesField.newSlowGeometryQuery(fieldName, luceneRelation, geometries);
            }
            return query;
        }

        private boolean isPointGeometry(LatLonGeometry[] geometries) {
            return geometries.length == 1 && geometries[0] instanceof org.apache.lucene.geo.Point;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return new LatLonPointIndexFieldData.Builder(name(), CoreValuesSourceType.GEOPOINT, GeoPointDocValuesField::new);
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherMultiGeoPointIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.GEOPOINT,
                    valueFetcher(sourcePaths, null, null),
                    searchLookup,
                    GeoPointDocValuesField::new
                );
            }

            throw new IllegalStateException("unknown field data type [" + operation.name() + "]");
        }

        @Override
        public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            GeoPoint originGeoPoint;
            if (origin instanceof GeoPoint) {
                originGeoPoint = (GeoPoint) origin;
            } else if (origin instanceof String) {
                originGeoPoint = GeoUtils.parseFromString((String) origin);
            } else {
                throw new IllegalArgumentException(
                    "Illegal type ["
                        + origin.getClass()
                        + "] for [origin]! "
                        + "Must be of type [geo_point] or [string] for geo_point fields!"
                );
            }
            double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
            if (isIndexed()) {
                // As we already apply boost in AbstractQueryBuilder::toQuery, we always passing a boost of 1.0 to distanceFeatureQuery
                return LatLonPoint.newDistanceFeatureQuery(name(), 1.0f, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
            } else {
                return new GeoPointScriptFieldDistanceFeatureQuery(
                    new Script(""),
                    ctx -> new SortedNumericDocValuesLongFieldScript(name(), context.lookup(), ctx),
                    name(),
                    originGeoPoint.lat(),
                    originGeoPoint.lon(),
                    pivotDouble
                );
            }
        }

        /**
         * If field is a time series metric field, returns its metric type
         * @return the metric type or null
         */
        @Override
        public TimeSeriesParams.MetricType getMetricType() {
            return metricType;
        }
    }

    /** GeoPoint parser implementation */
    private static class GeoPointParser extends PointParser<GeoPoint> {

        GeoPointParser(
            String field,
            CheckedFunction<XContentParser, GeoPoint, IOException> objectParser,
            GeoPoint nullValue,
            boolean ignoreZValue,
            boolean ignoreMalformed,
            boolean allowMultipleValues
        ) {
            super(field, objectParser, nullValue, ignoreZValue, ignoreMalformed, allowMultipleValues);
        }

        protected GeoPoint validate(GeoPoint in) {
            if (ignoreMalformed == false) {
                if (in.lat() > 90.0 || in.lat() < -90.0) {
                    throw new IllegalArgumentException("illegal latitude value [" + in.lat() + "] for " + field);
                }
                if (in.lon() > 180.0 || in.lon() < -180) {
                    throw new IllegalArgumentException("illegal longitude value [" + in.lon() + "] for " + field);
                }
            } else {
                if (isNormalizable(in.lat()) && isNormalizable(in.lon())) {
                    GeoUtils.normalizePoint(in);
                } else {
                    throw new ElasticsearchParseException("cannot normalize the point - not a number");
                }
            }
            return in;
        }

        private static boolean isNormalizable(double coord) {
            return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
        }

        @Override
        protected GeoPoint createPoint(double x, double y) {
            return new GeoPoint(y, x);
        }

        @Override
        public GeoPoint normalizeFromSource(GeoPoint point) {
            // normalize during parsing
            return point;
        }
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (hasScript()) {
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }
        if (fieldType().hasDocValues() == false) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it doesn't have doc values"
            );
        }
        if (ignoreMalformed()) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it ignores malformed points"
            );
        }
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        return new SortedNumericDocValuesSyntheticFieldLoader(name(), simpleName(), ignoreMalformed()) {
            final GeoPoint point = new GeoPoint();

            @Override
            protected void writeValue(XContentBuilder b, long value) throws IOException {
                point.reset(GeoEncodingUtils.decodeLatitude((int) (value >>> 32)), GeoEncodingUtils.decodeLongitude((int) value));
                point.toXContent(b, ToXContent.EMPTY_PARAMS);
            }
        };
    }
}
