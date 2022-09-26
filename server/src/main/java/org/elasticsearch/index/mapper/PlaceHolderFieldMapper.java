/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Mapper that is used to map existing fields in legacy indices (older than N-1) that
 * the current version of ES can't access anymore. Mapping these fields is important
 * so that the original mapping can be preserved and proper exception messages can
 * be provided when accessing these fields.
 */
public class PlaceHolderFieldMapper extends FieldMapper {

    public static final Function<String, TypeParser> PARSER = type -> new TypeParser((n, c) -> new Builder(n, type));

    public static class Builder extends FieldMapper.Builder {

        private final String type;

        // Parameters of legacy indices that are not interpreted by the current ES version. We still need to capture them here so that
        // they can be preserved in the mapping that is serialized out based on the toXContent method.
        // We use LinkedHashMap to preserve the parameter order.
        protected final Map<String, Object> unknownParams = new LinkedHashMap<>();

        public Builder(String name, String type) {
            super(name);
            this.type = type;
        }

        @Override
        public FieldMapper.Builder init(FieldMapper initializer) {
            assert initializer instanceof PlaceHolderFieldMapper;
            unknownParams.putAll(((PlaceHolderFieldMapper) initializer).unknownParams);
            return super.init(initializer);
        }

        @Override
        protected void merge(FieldMapper in, Conflicts conflicts, MapperBuilderContext mapperBuilderContext) {
            assert in instanceof PlaceHolderFieldMapper;
            unknownParams.putAll(((PlaceHolderFieldMapper) in).unknownParams);
            super.merge(in, conflicts, mapperBuilderContext);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder = super.toXContent(builder, params);
            for (Map.Entry<String, Object> unknownParam : unknownParams.entrySet()) {
                builder.field(unknownParam.getKey(), unknownParam.getValue());
            }
            return builder;
        }

        @Override
        protected void handleUnknownParamOnLegacyIndex(String propName, Object propNode) {
            unknownParams.put(propName, propNode);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return EMPTY_PARAMETERS;
        }

        @Override
        public PlaceHolderFieldMapper build(MapperBuilderContext context) {
            PlaceHolderFieldType mappedFieldType = new PlaceHolderFieldType(context.buildFullName(name), type, Map.of());
            return new PlaceHolderFieldMapper(
                name,
                mappedFieldType,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                unknownParams
            );
        }
    }

    public static final class PlaceHolderFieldType extends MappedFieldType {

        private String type;

        public PlaceHolderFieldType(String name, String type, Map<String, String> meta) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.type = type;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // ignore format parameter
            return new SourceValueFetcher(name(), context) {

                @Override
                protected Object parseSourceValue(Object value) {
                    // preserve as is, we can't really do anything smarter than that here
                    return value;
                }
            };
        }

        @Override
        public String typeName() {
            return type;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("term query"));
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, @Nullable SearchExecutionContext context) {
            throw new QueryShardException(context, fail("case insensitive term query"));
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            ZoneId timeZone,
            DateMathParser parser,
            SearchExecutionContext context
        ) {
            throw new QueryShardException(context, fail("range query"));
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context
        ) {
            throw new QueryShardException(context, fail("fuzzy query"));
        }

        @Override
        public Query prefixQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            SearchExecutionContext context
        ) {
            throw new QueryShardException(context, fail("prefix query"));
        }

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            SearchExecutionContext context
        ) {
            throw new QueryShardException(context, fail("wildcard query"));
        }

        @Override
        public Query normalizedWildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("normalized wildcard query"));
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            @Nullable MultiTermQuery.RewriteMethod method,
            SearchExecutionContext context
        ) {
            throw new QueryShardException(context, fail("regexp query"));
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("phrase query"));
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("multi-phrase query"));
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
            throw new QueryShardException(context, fail("phrase prefix query"));
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("span prefix query"));
        }

        @Override
        public Query distanceFeatureQuery(Object origin, String pivot, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("distance feature query"));
        }

        @Override
        public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("term intervals query"));
        }

        @Override
        public IntervalsSource prefixIntervals(BytesRef prefix, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("term intervals query"));
        }

        @Override
        public IntervalsSource fuzzyIntervals(
            String term,
            int maxDistance,
            int prefixLength,
            boolean transpositions,
            SearchExecutionContext context
        ) {
            throw new QueryShardException(context, fail("fuzzy intervals query"));
        }

        @Override
        public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
            throw new QueryShardException(context, fail("wildcard intervals query"));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException(fail("aggregation or sorts"));
        }

        private String fail(String query) {
            return "can't run " + query + " on field type " + type + " of legacy index";
        }
    }

    protected final Map<String, Object> unknownParams = new LinkedHashMap<>();

    public PlaceHolderFieldMapper(
        String simpleName,
        PlaceHolderFieldType fieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Map<String, Object> unknownParams
    ) {
        super(simpleName, fieldType, multiFields, copyTo);
        this.unknownParams.putAll(unknownParams);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        throw new IllegalArgumentException("can't parse value for placeholder field type");
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new PlaceHolderFieldMapper.Builder(simpleName(), typeName()).init(this);
    }

    @Override
    protected String contentType() {
        return typeName();
    }
}
