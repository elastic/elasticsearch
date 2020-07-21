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
        return ScriptFieldMapper.CONTENT_TYPE;
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

    public abstract Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    );

    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        QueryShardContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use fuzzy queries on keyword and text fields - not on [" + name() + "] which is of type [" + runtimeType() + "]"
        );
    }

    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Can only use prefix queries on keyword, text and wildcard fields - not on ["
                + name()
                + "] which is of type ["
                + runtimeType()
                + "]"
        );
    }

    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Can only use wildcard queries on keyword, text and wildcard fields - not on ["
                + name()
                + "] which is of type ["
                + runtimeType()
                + "]"
        );
    }

    public Query regexpQuery(
        String value,
        int flags,
        int maxDeterminizedStates,
        MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use regexp queries on keyword and text fields - not on [" + name() + "] which is of type [" + runtimeType() + "]"
        );
    }

    public abstract Query existsQuery(QueryShardContext context);

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase queries on text fields - not on [" + name() + "] which is of type [" + runtimeType() + "]"
        );
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase queries on text fields - not on [" + name() + "] which is of type [" + runtimeType() + "]"
        );
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase prefix queries on text fields - not on [" + name() + "] which is of type [" + runtimeType() + "]"
        );
    }

    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Can only use span prefix queries on text fields - not on [" + name() + "] which is of type [" + runtimeType() + "]"
        );
    }

    protected final void checkAllowExpensiveQueries(QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "queries cannot be executed against ["
                    + ScriptFieldMapper.CONTENT_TYPE
                    + "] fields while ["
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "] is set to [false]."
            );
        }
    }
}
