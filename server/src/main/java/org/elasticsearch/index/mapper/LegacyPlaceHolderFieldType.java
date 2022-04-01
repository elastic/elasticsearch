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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A placeholder for legacy field types that can no longer be searched
 */
public final class LegacyPlaceHolderFieldType extends MappedFieldType {

    private final String type;

    public LegacyPlaceHolderFieldType(String name, String type, Map<String, String> meta) {
        super(name, false, false, false, TextSearchInfo.NONE, meta);
        this.type = type;
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        throw new UnsupportedOperationException("can't fetch values on place holder field type");
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
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        throw new IllegalArgumentException(fail("aggregation or sorts"));
    }

    private String fail(String query) {
        return "can't run " + query + " on field type " + type + " of legacy index";
    }
}
