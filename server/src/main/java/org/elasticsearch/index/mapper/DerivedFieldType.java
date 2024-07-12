/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.management.Query;

/**
 * MappedFieldType for Derived Fields
 * Contains logic to execute different type of queries on a derived field of given type.
 * @opensearch.internal
 */

public abstract class DerivedFieldType extends MappedFieldType implements GeoShapeQueryable {
    final DerivedField derivedField;
    final FieldMapper typeFieldMapper;
    final Function<Object, IndexableField> indexableFieldGenerator;

    public DerivedFieldType(
        DerivedField derivedField,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        Map<String, String> meta,
        FieldMapper typeFieldMapper,
        Function<Object, IndexableField> fieldFunction
    ) {
        super(derivedField.getName(), isIndexed, isStored, hasDocValues, typeFieldMapper.fieldType().getTextSearchInfo(), meta);
        this.derivedField = derivedField;
        this.typeFieldMapper = typeFieldMapper;
        this.indexableFieldGenerator = fieldFunction;
    }

    public DerivedFieldType(
        DerivedField derivedField,
        FieldMapper typeFieldMapper,
        Function<Object, IndexableField> fieldFunction,
        IndexAnalyzers indexAnalyzers
    ) {
        this(derivedField, false, false, false, Collections.emptyMap(), typeFieldMapper, fieldFunction);
    }

    @Override
    public TextSearchInfo getTextSearchInfo() {
        return typeFieldMapper.fieldType().getTextSearchInfo();
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    TextFieldMapper.TextFieldType getPrefilterFieldType(QueryShardContext context) {
        if (derivedField.getPrefilterField() == null || derivedField.getPrefilterField().isEmpty()) {
            return null;
        }
        MappedFieldType mappedFieldType = context.fieldMapper(derivedField.getPrefilterField());
        if (mappedFieldType == null) {
            throw new MapperException("prefilter_field[" + derivedField.getPrefilterField() + "] is not defined in the index mappings");
        }
        if (!(mappedFieldType instanceof TextFieldMapper.TextFieldType)) {
            throw new MapperException(
                "prefilter_field["
                    + derivedField.getPrefilterField()
                    + "] should be of type text. Type found ["
                    + mappedFieldType.typeName()
                    + "]."
            );
        }
        return (TextFieldMapper.TextFieldType) mappedFieldType;
    }

    @Override
    public String typeName() {
        return "derived";
    }

    public String getType() {
        return derivedField.getType();
    }

    public FieldMapper getFieldMapper() {
        return typeFieldMapper;
    }

    public Function<Object, IndexableField> getIndexableFieldGenerator() {
        return indexableFieldGenerator;
    }

    public NamedAnalyzer getIndexAnalyzer() {
        return getFieldMapper().mappedFieldType.indexAnalyzer();
    }

    @Override
    public DerivedFieldValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        Function<Object, Object> valueForDisplay = DerivedFieldSupportedTypes.getValueForDisplayGenerator(
            getType(),
            derivedField.getFormat() != null ? DateFormatter.forPattern(derivedField.getFormat()) : null
        );
        return new DerivedFieldValueFetcher(
            getDerivedFieldLeafFactory(derivedField.getScript(), context, searchLookup == null ? context.lookup() : searchLookup),
            valueForDisplay
        );
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        Query query = typeFieldMapper.mappedFieldType.termQuery(value, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(prefilterFieldType -> createConjuctionQuery(prefilterFieldType.termQuery(value, context), derivedFieldQuery))
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, @Nullable QueryShardContext context) {
        Query query = typeFieldMapper.mappedFieldType.termQueryCaseInsensitive(value, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(prefilterFieldType.termQueryCaseInsensitive(value, context), derivedFieldQuery)
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query termsQuery(List<?> values, @Nullable QueryShardContext context) {
        Query query = typeFieldMapper.mappedFieldType.termsQuery(values, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(prefilterFieldType -> createConjuctionQuery(prefilterFieldType.termsQuery(values, context), derivedFieldQuery))
            .orElse(derivedFieldQuery);
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
        QueryShardContext context
    ) {
        Query query = typeFieldMapper.mappedFieldType.rangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            relation,
            timeZone,
            parser,
            context
        );
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        return new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        QueryShardContext context
    ) {
        Query query = typeFieldMapper.mappedFieldType.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(
                    prefilterFieldType.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context),
                    derivedFieldQuery
                )
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        @Nullable MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        Query query = typeFieldMapper.mappedFieldType.fuzzyQuery(
            value,
            fuzziness,
            prefixLength,
            maxExpansions,
            transpositions,
            method,
            context
        );
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(
                    prefilterFieldType.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, method, context),
                    derivedFieldQuery
                )
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query prefixQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitive,
        QueryShardContext context
    ) {
        Query query = typeFieldMapper.mappedFieldType.prefixQuery(value, method, caseInsensitive, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(
                    prefilterFieldType.prefixQuery(value, method, caseInsensitive, context),
                    derivedFieldQuery
                )
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query wildcardQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitive,
        QueryShardContext context
    ) {
        Query query = typeFieldMapper.mappedFieldType.wildcardQuery(value, method, caseInsensitive, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(
                    prefilterFieldType.wildcardQuery(value, method, caseInsensitive, context),
                    derivedFieldQuery
                )
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query normalizedWildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        Query query = typeFieldMapper.mappedFieldType.normalizedWildcardQuery(value, method, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(
                    prefilterFieldType.normalizedWildcardQuery(value, method, context),
                    derivedFieldQuery
                )
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        Query query = typeFieldMapper.mappedFieldType.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context))
            .map(
                prefilterFieldType -> createConjuctionQuery(
                    prefilterFieldType.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context),
                    derivedFieldQuery
                )
            )
            .orElse(derivedFieldQuery);
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context) throws IOException {
        Query query = typeFieldMapper.mappedFieldType.phraseQuery(stream, slop, enablePositionIncrements, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context)).map(prefilterFieldType -> {
            try {
                return createConjuctionQuery(
                    prefilterFieldType.phraseQuery(stream, slop, enablePositionIncrements, context),
                    derivedFieldQuery
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).orElse(derivedFieldQuery);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context)
        throws IOException {
        Query query = typeFieldMapper.mappedFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context)).map(prefilterFieldType -> {
            try {
                return createConjuctionQuery(
                    prefilterFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, context),
                    derivedFieldQuery
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).orElse(derivedFieldQuery);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, QueryShardContext context) throws IOException {
        Query query = typeFieldMapper.mappedFieldType.phrasePrefixQuery(stream, slop, maxExpansions, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        DerivedFieldQuery derivedFieldQuery = new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
        return Optional.ofNullable(getPrefilterFieldType(context)).map(prefilterFieldType -> {
            try {
                return createConjuctionQuery(prefilterFieldType.phrasePrefixQuery(stream, slop, maxExpansions, context), derivedFieldQuery);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).orElse(derivedFieldQuery);
    }

    @Override
    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Can only use span prefix queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    @Override
    public Query distanceFeatureQuery(Object origin, String pivot, float boost, QueryShardContext context) {
        Query query = typeFieldMapper.mappedFieldType.distanceFeatureQuery(origin, pivot, boost, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        return new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
    }

    @Override
    public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        Query query = ((GeoShapeQueryable) (typeFieldMapper.mappedFieldType)).geoShapeQuery(shape, fieldName, relation, context);
        DerivedFieldValueFetcher valueFetcher = valueFetcher(context, context.lookup(), null);
        return new DerivedFieldQuery(
            query,
            valueFetcher,
            context.lookup(),
            getIndexAnalyzer(),
            indexableFieldGenerator,
            derivedField.getIgnoreMalformed()
        );
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support exist queries");
    }

    @Override
    public boolean isAggregatable() {
        return false;
    }

    private Query createConjuctionQuery(Query filterQuery, DerivedFieldQuery derivedFieldQuery) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(filterQuery, BooleanClause.Occur.FILTER);
        builder.add(derivedFieldQuery, BooleanClause.Occur.FILTER);
        return builder.build();
    }

    public static DerivedFieldScript.LeafFactory getDerivedFieldLeafFactory(
        Script script,
        QueryShardContext context,
        SearchLookup searchLookup
    ) {
        if (!context.documentMapper("").sourceMapper().enabled()) {
            throw new IllegalArgumentException(
                "DerivedFieldQuery error: unable to fetch fields from _source field: _source is disabled in the mappings "
                    + "for index ["
                    + context.index().getName()
                    + "]"
            );
        }
        DerivedFieldScript.Factory factory = context.compile(script, DerivedFieldScript.CONTEXT);
        return factory.newFactory(script.getParams(), searchLookup);
    }
}
