/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SimpleVectorTileFormatter;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.core.CheckedConsumer;
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
import org.elasticsearch.search.aggregations.support.TimeSeriesValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.xcontent.CopyingXContentParser;
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

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;

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

    public static final class Builder extends FieldMapper.Builder {

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<GeoPoint> nullValue;
        final Parameter<Boolean> indexed;
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);
        private final Parameter<Script> script = Parameter.scriptParam(m -> builder(m).script.get());
        private final Parameter<OnScriptError> onScriptErrorParam = Parameter.onScriptErrorParam(
            m -> builder(m).onScriptErrorParam.get(),
            script
        );
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
                onScriptErrorParam,
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
                : (lookup, ctx, doc, consumer) -> factory.newFactory(leafName(), script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            boolean ignoreMalformedEnabled = ignoreMalformed.get().value();
            Parser<GeoPoint> geoParser = new GeoPointParser(
                leafName(),
                (parser) -> GeoUtils.parseGeoPoint(parser, ignoreZValue.get().value()),
                nullValue.get(),
                ignoreZValue.get().value(),
                ignoreMalformedEnabled,
                metric.get() != TimeSeriesParams.MetricType.POSITION,
                context.isSourceSynthetic() && ignoreMalformedEnabled
            );
            GeoPointFieldType ft = new GeoPointFieldType(
                context.buildFullName(leafName()),
                indexed.get() && indexCreatedVersion.isLegacyIndexVersion() == false,
                stored.get(),
                hasDocValues.get(),
                geoParser,
                nullValue.get(),
                scriptValues(),
                meta.get(),
                metric.get(),
                indexMode,
                context.isSourceSynthetic()
            );
            hasScript = script.get() != null;
            onScriptError = onScriptErrorParam.get();
            return new GeoPointFieldMapper(leafName(), ft, builderParams(this, context), geoParser, this);
        }

    }

    public static final TypeParser PARSER = createTypeParserWithLegacySupport(
        (n, c) -> new Builder(
            n,
            c.scriptCompiler(),
            IGNORE_MALFORMED_SETTING.get(c.getSettings()),
            c.indexVersionCreated(),
            c.getIndexSettings().getMode()
        )
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
        BuilderParams builderParams,
        Parser<GeoPoint> parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            builderParams,
            builder.ignoreMalformed.get(),
            builder.ignoreZValue.get(),
            builder.nullValue.get(),
            parser
        );
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
            leafName(),
            builder.scriptCompiler,
            builder.ignoreMalformed.getDefaultValue().value(),
            indexCreatedVersion,
            indexMode
        ).init(this);
    }

    @Override
    protected void index(DocumentParserContext context, GeoPoint geometry) throws IOException {
        final boolean indexed = fieldType().isIndexed();
        final boolean hasDocValues = fieldType().hasDocValues();
        final boolean store = fieldType().isStored();
        if (indexed && hasDocValues) {
            context.doc().add(new LatLonPointWithDocValues(fieldType().name(), geometry.lat(), geometry.lon()));
        } else if (hasDocValues) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), geometry.lat(), geometry.lon()));
        } else if (indexed) {
            context.doc().add(new LatLonPoint(fieldType().name(), geometry.lat(), geometry.lon()));
        }
        if (store) {
            context.doc().add(new StoredField(fieldType().name(), geometry.toString()));
        }
        if (hasDocValues == false && (indexed || store)) {
            // When the field doesn't have doc values so that we can run exists queries, we also need to index the field name separately.
            context.addToFieldNames(fieldType().name());
        }

        // TODO phase out geohash (which is currently used in the CompletionSuggester)
        // we only expose the geohash value and disallow advancing tokens, hence we can reuse the same parser throughout multiple sub-fields
        DocumentParserContext parserContext = context.switchParser(new GeoHashMultiFieldParser(context.parser(), geometry.geohash()));
        multiFields().parse(this, context, () -> parserContext);
    }

    /**
     * Parser that pretends to be the main document parser, but exposes the provided geohash regardless of how the geopoint was provided
     * in the incoming document. We rely on the fact that consumers only ever read text from the parser and never
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
        public String text() throws IOException {
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

    public static class GeoPointFieldType extends AbstractPointFieldType<GeoPoint> implements GeoShapeQueryable {
        private final TimeSeriesParams.MetricType metricType;

        public static final GeoFormatterFactory<GeoPoint> GEO_FORMATTER_FACTORY = new GeoFormatterFactory<>(
            List.of(new SimpleVectorTileFormatter())
        );

        private final FieldValues<GeoPoint> scriptValues;
        private final IndexMode indexMode;
        private final boolean isSyntheticSource;

        private GeoPointFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Parser<GeoPoint> parser,
            GeoPoint nullValue,
            FieldValues<GeoPoint> scriptValues,
            Map<String, String> meta,
            TimeSeriesParams.MetricType metricType,
            IndexMode indexMode,
            boolean isSyntheticSource
        ) {
            super(name, indexed, stored, hasDocValues, parser, nullValue, meta);
            this.scriptValues = scriptValues;
            this.metricType = metricType;
            this.indexMode = indexMode;
            this.isSyntheticSource = isSyntheticSource;
        }

        // only used in test
        public GeoPointFieldType(String name, TimeSeriesParams.MetricType metricType, IndexMode indexMode) {
            this(name, true, false, true, null, null, null, Collections.emptyMap(), metricType, indexMode, false);
        }

        // only used in test
        public GeoPointFieldType(String name) {
            this(name, null, null);
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

        private static boolean isPointGeometry(LatLonGeometry[] geometries) {
            return geometries.length == 1 && geometries[0] instanceof org.apache.lucene.geo.Point;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();

            if (operation == FielddataOperation.SEARCH) {
                failIfNoDocValues();
            }

            ValuesSourceType valuesSourceType = indexMode == IndexMode.TIME_SERIES && metricType == TimeSeriesParams.MetricType.POSITION
                ? TimeSeriesValuesSourceType.POSITION
                : CoreValuesSourceType.GEOPOINT;

            if ((operation == FielddataOperation.SEARCH || operation == FielddataOperation.SCRIPT) && hasDocValues()) {
                return new LatLonPointIndexFieldData.Builder(name(), valuesSourceType, GeoPointDocValuesField::new);
            }

            if (operation == FielddataOperation.SCRIPT) {
                SearchLookup searchLookup = fieldDataContext.lookupSupplier().get();
                Set<String> sourcePaths = fieldDataContext.sourcePathsLookup().apply(name());

                return new SourceValueFetcherMultiGeoPointIndexFieldData.Builder(
                    name(),
                    valuesSourceType,
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

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (blContext.fieldExtractPreference() == DOC_VALUES && hasDocValues()) {
                return new BlockDocValuesReader.LongsBlockLoader(name());
            }

            // There are two scenarios possible once we arrive here:
            //
            // * Stored source - we'll just use blockLoaderFromSource
            // * Synthetic source. However, because of the fieldExtractPreference() check above it is still possible that doc_values are
            // present here.
            // So we have two subcases:
            // - doc_values are enabled - _ignored_source field does not exist since we have doc_values. We will use
            // blockLoaderFromSource which reads "native" synthetic source.
            // - doc_values are disabled - we know that _ignored_source field is present and use a special block loader unless it's a multi
            // field.
            if (isSyntheticSource && hasDocValues() == false && blContext.parentField(name()) == null) {
                return blockLoaderFromFallbackSyntheticSource(blContext);
            }

            return blockLoaderFromSource(blContext);
        }
    }

    /** GeoPoint parser implementation */
    private static class GeoPointParser extends PointParser<GeoPoint> {
        private final boolean storeMalformedDataForSyntheticSource;

        GeoPointParser(
            String field,
            CheckedFunction<XContentParser, GeoPoint, IOException> objectParser,
            GeoPoint nullValue,
            boolean ignoreZValue,
            boolean ignoreMalformed,
            boolean allowMultipleValues,
            boolean storeMalformedDataForSyntheticSource
        ) {
            super(field, objectParser, nullValue, ignoreZValue, ignoreMalformed, allowMultipleValues);
            this.storeMalformedDataForSyntheticSource = storeMalformedDataForSyntheticSource;
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

        @Override
        protected void parseAndConsumeFromObject(
            XContentParser parser,
            CheckedConsumer<GeoPoint, IOException> consumer,
            MalformedValueHandler malformedHandler
        ) throws IOException {
            XContentParser parserWithCustomization = parser;
            XContentBuilder malformedDataForSyntheticSource = null;

            if (storeMalformedDataForSyntheticSource) {
                if (parser.currentToken() == XContentParser.Token.START_OBJECT
                    || parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    // We have a complex structure so we'll memorize it while parsing.
                    var copyingParser = new CopyingXContentParser(parser);
                    malformedDataForSyntheticSource = copyingParser.getBuilder();
                    parserWithCustomization = copyingParser;
                } else {
                    // We have a single value (e.g. a string) that is potentially malformed, let's simply remember it.
                    malformedDataForSyntheticSource = XContentBuilder.builder(parser.contentType().xContent()).copyCurrentStructure(parser);
                }
            }

            try {
                GeoPoint point = objectParser.apply(parserWithCustomization);
                consumer.accept(validate(point));
            } catch (Exception e) {
                malformedHandler.notify(e, malformedDataForSyntheticSource);
            }
        }
    }

    @Override
    protected void onMalformedValue(DocumentParserContext context, XContentBuilder malformedDataForSyntheticSource, Exception cause)
        throws IOException {
        super.onMalformedValue(context, malformedDataForSyntheticSource, cause);
        if (malformedDataForSyntheticSource != null) {
            context.doc().add(IgnoreMalformedStoredValues.storedField(fullPath(), malformedDataForSyntheticSource));
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (fieldType().hasDocValues()) {
            return new SyntheticSourceSupport.Native(
                () -> new SortedNumericDocValuesSyntheticFieldLoader(fullPath(), leafName(), ignoreMalformed()) {
                    final GeoPoint point = new GeoPoint();

                    @Override
                    protected void writeValue(XContentBuilder b, long value) throws IOException {
                        point.reset(GeoEncodingUtils.decodeLatitude((int) (value >>> 32)), GeoEncodingUtils.decodeLongitude((int) value));
                        point.toXContent(b, ToXContent.EMPTY_PARAMS);
                    }
                }
            );
        }

        return super.syntheticSourceSupport();
    }

    /**
     * Utility class that allows adding index and doc values in one field
     */
    public static class LatLonPointWithDocValues extends Field {

        public static final FieldType TYPE = new FieldType();

        static {
            TYPE.setDimensions(2, Integer.BYTES);
            TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
            TYPE.freeze();
        }

        // holds the doc value value.
        private final long docValue;

        public LatLonPointWithDocValues(String name, double latitude, double longitude) {
            super(name, TYPE);
            final byte[] bytes;
            if (fieldsData == null) {
                bytes = new byte[8];
                fieldsData = new BytesRef(bytes);
            } else {
                bytes = ((BytesRef) fieldsData).bytes;
            }

            final int latitudeEncoded = GeoEncodingUtils.encodeLatitude(latitude);
            final int longitudeEncoded = GeoEncodingUtils.encodeLongitude(longitude);
            NumericUtils.intToSortableBytes(latitudeEncoded, bytes, 0);
            NumericUtils.intToSortableBytes(longitudeEncoded, bytes, Integer.BYTES);
            docValue = (((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL);
        }

        @Override
        public Number numericValue() {
            return docValue;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append(getClass().getSimpleName());
            result.append(" <");
            result.append(name);
            result.append(':');

            byte[] bytes = ((BytesRef) fieldsData).bytes;
            result.append(GeoEncodingUtils.decodeLatitude(bytes, 0));
            result.append(',');
            result.append(GeoEncodingUtils.decodeLongitude(bytes, Integer.BYTES));

            result.append('>');
            return result.toString();
        }
    }
}
