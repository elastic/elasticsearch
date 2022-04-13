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
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SimpleVectorTileFormatter;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.GeoPointDocValuesField;
import org.elasticsearch.script.field.SortedNumericDocValuesLongFieldScript;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.GeoPointScriptFieldDistanceFeatureQuery;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends AbstractPointGeometryFieldMapper<GeoPoint> {

    public static final String CONTENT_TYPE = "geo_point";

    private static Builder builder(FieldMapper in) {
        return ((GeoPointFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<GeoPoint> nullValue;
        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);
        private final Parameter<Script> script = Parameter.scriptParam(m -> builder(m).script.get());
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> builder(m).onScriptError.get(), script);
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final ScriptCompiler scriptCompiler;
        private final Version indexCreatedVersion;

        public Builder(String name, ScriptCompiler scriptCompiler, boolean ignoreMalformedByDefault, Version indexCreatedVersion) {
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
            addScriptValidation(script, indexed, hasDocValues);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(hasDocValues, indexed, stored, ignoreMalformed, ignoreZValue, nullValue, script, onScriptError, meta);
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        private static GeoPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            if (nullValue == null) {
                return null;
            }
            GeoPoint point = new GeoPoint();
            GeoUtils.parseGeoPoint(nullValue, point, ignoreZValue);
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
                : (lookup, ctx, doc, consumer) -> factory.newFactory(name, script.get().getParams(), lookup)
                    .newInstance(ctx)
                    .runGeoPointForDoc(doc, consumer);
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            Parser<GeoPoint> geoParser = new GeoPointParser(name, GeoPoint::new, (parser, point) -> {
                GeoUtils.parseGeoPoint(parser, point, ignoreZValue.get().value());
                return point;
            }, nullValue.get(), ignoreZValue.get().value(), ignoreMalformed.get().value());
            GeoPointFieldType ft = new GeoPointFieldType(
                context.buildFullName(name),
                indexed.get() && indexCreatedVersion.isLegacyIndexVersion() == false,
                stored.get(),
                hasDocValues.get(),
                geoParser,
                scriptValues(),
                meta.get()
            );
            if (this.script.get() == null) {
                return new GeoPointFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), geoParser, this);
            }
            return new GeoPointFieldMapper(name, ft, geoParser, this);
        }

    }

    public static TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.scriptCompiler(), IGNORE_MALFORMED_SETTING.get(c.getSettings()), c.indexVersionCreated())
    );

    private final Builder builder;
    private final FieldValues<GeoPoint> scriptValues;
    private final Version indexCreatedVersion;

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
    }

    public GeoPointFieldMapper(String simpleName, MappedFieldType mappedFieldType, Parser<GeoPoint> parser, Builder builder) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty(), parser, builder.onScriptError.get());
        this.builder = builder;
        this.scriptValues = builder.scriptValues();
        this.indexCreatedVersion = builder.indexCreatedVersion;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), builder.scriptCompiler, builder.ignoreMalformed.getDefaultValue().value(), indexCreatedVersion)
            .init(this);
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

        private static final GeoFormatterFactory<GeoPoint> GEO_FORMATTER_FACTORY = new GeoFormatterFactory<>(
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
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, parser, meta);
            this.scriptValues = scriptValues;
        }

        // only used in test
        public GeoPointFieldType(String name) {
            this(name, true, false, true, null, null, Collections.emptyMap());
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
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            final LatLonGeometry[] luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, shape, relation);
            if (luceneGeometries.length == 0) {
                return new MatchNoDocsQuery();
            }
            final ShapeField.QueryRelation luceneRelation;
            if (shape.type() == ShapeType.POINT && relation == ShapeRelation.INTERSECTS) {
                // For point queries and intersects, lucene does not match points that are encoded to Integer.MAX_VALUE.
                // We use contains instead.
                luceneRelation = ShapeField.QueryRelation.CONTAINS;
            } else {
                luceneRelation = relation.getLuceneRelation();
            }
            Query query;
            if (isIndexed()) {
                query = LatLonPoint.newGeometryQuery(fieldName, luceneRelation, luceneGeometries);
                if (hasDocValues()) {
                    Query dvQuery = LatLonDocValuesField.newSlowGeometryQuery(fieldName, luceneRelation, luceneGeometries);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                }
            } else {
                query = LatLonDocValuesField.newSlowGeometryQuery(fieldName, luceneRelation, luceneGeometries);
            }
            return query;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new AbstractLatLonPointIndexFieldData.Builder(name(), CoreValuesSourceType.GEOPOINT, GeoPointDocValuesField::new);
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
    }

    /** GeoPoint parser implementation */
    private static class GeoPointParser extends PointParser<GeoPoint> {

        GeoPointParser(
            String field,
            Supplier<GeoPoint> pointSupplier,
            CheckedBiFunction<XContentParser, GeoPoint, GeoPoint, IOException> objectParser,
            GeoPoint nullValue,
            boolean ignoreZValue,
            boolean ignoreMalformed
        ) {
            super(field, pointSupplier, objectParser, nullValue, ignoreZValue, ignoreMalformed);
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
        protected void reset(GeoPoint in, double x, double y) {
            in.reset(y, x);
        }

        @Override
        public GeoPoint normalizeFromSource(GeoPoint point) {
            // normalize during parsing
            return point;
        }
    }
}
