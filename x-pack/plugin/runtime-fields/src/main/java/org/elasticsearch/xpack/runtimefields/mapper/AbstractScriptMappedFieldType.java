/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Abstract base {@linkplain MappedFieldType} for scripted fields.
 */
abstract class AbstractScriptMappedFieldType extends MappedFieldType {
    protected final Script script;

    AbstractScriptMappedFieldType(String name, Script script, Map<String, String> meta) {
        super(name, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        this.script = script;
    }

    protected abstract String runtimeType();

    @Override
    public final String typeName() {
        return RuntimeFieldMapper.CONTENT_TYPE;
    }

    @Override
    public final String familyTypeName() {
        return runtimeType();
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
    }

    public abstract Query termsQuery(List<?> values, QueryShardContext context);

    @Override
    public final Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    ) {
        if (relation == ShapeRelation.DISJOINT) {
            String message = "Field [%s] of type [%s] with runtime type [%s] does not support DISJOINT ranges";
            throw new IllegalArgumentException(String.format(Locale.ROOT, message, name(), typeName(), runtimeType()));
        }
        return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, timeZone, parser, context);
    }

    protected abstract Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    );

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        QueryShardContext context
    ) {
        throw new IllegalArgumentException(unsupported("fuzzy", "keyword and text"));
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(unsupported("prefix", "keyword, text and wildcard"));
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(unsupported("wildcard", "keyword, text and wildcard"));
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        throw new IllegalArgumentException(unsupported("regexp", "keyword and text"));
    }

    @Override
    public abstract Query existsQuery(QueryShardContext context);

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException(unsupported("phrase", "text"));
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException(unsupported("phrase", "text"));
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        throw new IllegalArgumentException(unsupported("phrase prefix", "text"));
    }

    @Override
    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(unsupported("span prefix", "text"));
    }

    private String unsupported(String query, String supported) {
        String thisField = "[" + name() + "] which is of type [script] with runtime_type [" + runtimeType() + "]";
        return "Can only use " + query + " queries on " + supported + " fields - not on " + thisField;
    }

    protected final void checkAllowExpensiveQueries(QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "queries cannot be executed against ["
                    + RuntimeFieldMapper.CONTENT_TYPE
                    + "] fields while ["
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "] is set to [false]."
            );
        }
    }

    /**
     * The format that this field should use. The default implementation is
     * {@code null} because most fields don't support formats.
     */
    protected String format() {
        return null;
    }

    /**
     * The locale that this field's format should use. The default
     * implementation is {@code null} because most fields don't
     * support formats.
     */
    protected Locale formatLocale() {
        return null;
    }
}
