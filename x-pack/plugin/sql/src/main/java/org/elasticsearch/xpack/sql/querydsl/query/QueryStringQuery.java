/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Map.entry;

public class QueryStringQuery extends LeafQuery {

    // TODO: it'd be great if these could be constants instead of Strings, needs a core change to make the fields public first
    private static final Map<String, BiConsumer<QueryStringQueryBuilder, String>> BUILDER_APPLIERS = Map.ofEntries(
            entry("allow_leading_wildcard", (qb, s) -> qb.allowLeadingWildcard(Booleans.parseBoolean(s))),
            entry("analyze_wildcard", (qb, s) -> qb.analyzeWildcard(Booleans.parseBoolean(s))),
            entry("analyzer", QueryStringQueryBuilder::analyzer),
            entry("auto_generate_synonyms_phrase_query", (qb, s) -> qb.autoGenerateSynonymsPhraseQuery(Booleans.parseBoolean(s))),
            entry("default_field", QueryStringQueryBuilder::defaultField),
            entry("default_operator", (qb, s) -> qb.defaultOperator(Operator.fromString(s))),
            entry("enable_position_increments", (qb, s) -> qb.enablePositionIncrements(Booleans.parseBoolean(s))),
            entry("escape", (qb, s) -> qb.escape(Booleans.parseBoolean(s))),
            entry("fuzziness", (qb, s) -> qb.fuzziness(Fuzziness.fromString(s))),
            entry("fuzzy_max_expansions", (qb, s) -> qb.fuzzyMaxExpansions(Integer.valueOf(s))),
            entry("fuzzy_prefix_length", (qb, s) -> qb.fuzzyPrefixLength(Integer.valueOf(s))),
            entry("fuzzy_rewrite", QueryStringQueryBuilder::fuzzyRewrite),
            entry("fuzzy_transpositions", (qb, s) -> qb.fuzzyTranspositions(Booleans.parseBoolean(s))),
            entry("lenient", (qb, s) -> qb.lenient(Booleans.parseBoolean(s))),
            entry("max_determinized_states", (qb, s) -> qb.maxDeterminizedStates(Integer.valueOf(s))),
            entry("minimum_should_match", QueryStringQueryBuilder::minimumShouldMatch),
            entry("phrase_slop", (qb, s) -> qb.phraseSlop(Integer.valueOf(s))),
            entry("rewrite", QueryStringQueryBuilder::rewrite),
            entry("quote_analyzer", QueryStringQueryBuilder::quoteAnalyzer),
            entry("quote_field_suffix", QueryStringQueryBuilder::quoteFieldSuffix),
            entry("tie_breaker", (qb, s) -> qb.tieBreaker(Float.valueOf(s))),
            entry("time_zone", QueryStringQueryBuilder::timeZone),
            entry("type", (qb, s) -> qb.type(MultiMatchQueryBuilder.Type.parse(s, LoggingDeprecationHandler.INSTANCE))));

    private final String query;
    private final Map<String, Float> fields;
    private StringQueryPredicate predicate;
    private final Map<String, String> options;

    // dedicated constructor for QueryTranslator
    public QueryStringQuery(Source source, String query, String fieldName) {
        this(source, query, Collections.singletonMap(fieldName, Float.valueOf(1.0f)), null);
    }

    public QueryStringQuery(Source source, String query, Map<String, Float> fields, StringQueryPredicate predicate) {
        super(source);
        this.query = query;
        this.fields = fields;
        this.predicate = predicate;
        this.options = predicate == null ? Collections.emptyMap() : predicate.optionMap();
    }

    @Override
    public QueryBuilder asBuilder() {
        final QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(query);
        queryBuilder.fields(fields);
        options.forEach((k, v) -> {
            if (BUILDER_APPLIERS.containsKey(k)) {
                BUILDER_APPLIERS.get(k).accept(queryBuilder, v);
            } else {
                throw new IllegalArgumentException("illegal query_string option [" + k + "]");
            }
        });
        return queryBuilder;
    }

    public Map<String, Float> fields() {
        return fields;
    }

    public String query() {
        return query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, fields, predicate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryStringQuery other = (QueryStringQuery) obj;
        return Objects.equals(query, other.query)
                && Objects.equals(fields, other.fields)
                && Objects.equals(predicate, other.predicate);
    }

    @Override
    protected String innerToString() {
        return fields + ":" + query;
    }
}
