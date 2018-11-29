/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public class QueryStringQuery extends LeafQuery {

    private static final Map<String, BiConsumer<QueryStringQueryBuilder, String>> BUILDER_APPLIERS;

    static {
        HashMap<String, BiConsumer<QueryStringQueryBuilder, String>> appliers = new HashMap<>(28);
        // TODO: it'd be great if these could be constants instead of Strings, needs a core change to make the fields public first
        appliers.put("default_field", (qb, s) -> qb.defaultField(s));
        appliers.put("default_operator", (qb, s) -> qb.defaultOperator(Operator.fromString(s)));
        appliers.put("analyzer", (qb, s) -> qb.analyzer(s));
        appliers.put("quote_analyzer", (qb, s) -> qb.quoteAnalyzer(s));
        appliers.put("allow_leading_wildcard", (qb, s) -> qb.allowLeadingWildcard(Booleans.parseBoolean(s)));
        appliers.put("max_determinized_states", (qb, s) -> qb.maxDeterminizedStates(Integer.valueOf(s)));
        appliers.put("lowercase_expanded_terms", (qb, s) -> {});
        appliers.put("enable_position_increments", (qb, s) -> qb.enablePositionIncrements(Booleans.parseBoolean(s)));
        appliers.put("escape", (qb, s) -> qb.escape(Booleans.parseBoolean(s)));
        appliers.put("fuzzy_prefix_length", (qb, s) -> qb.fuzzyPrefixLength(Integer.valueOf(s)));
        appliers.put("fuzzy_max_expansions", (qb, s) -> qb.fuzzyMaxExpansions(Integer.valueOf(s)));
        appliers.put("fuzzy_rewrite", (qb, s) -> qb.fuzzyRewrite(s));
        appliers.put("phrase_slop", (qb, s) -> qb.phraseSlop(Integer.valueOf(s)));
        appliers.put("tie_breaker", (qb, s) -> qb.tieBreaker(Float.valueOf(s)));
        appliers.put("analyze_wildcard", (qb, s) -> qb.analyzeWildcard(Booleans.parseBoolean(s)));
        appliers.put("rewrite", (qb, s) -> qb.rewrite(s));
        appliers.put("minimum_should_match", (qb, s) -> qb.minimumShouldMatch(s));
        appliers.put("quote_field_suffix", (qb, s) -> qb.quoteFieldSuffix(s));
        appliers.put("lenient", (qb, s) -> qb.lenient(Booleans.parseBoolean(s)));
        appliers.put("locale", (qb, s) -> {});
        appliers.put("time_zone", (qb, s) -> qb.timeZone(s));
        appliers.put("type", (qb, s) -> qb.type(MultiMatchQueryBuilder.Type.parse(s, LoggingDeprecationHandler.INSTANCE)));
        appliers.put("auto_generate_synonyms_phrase_query", (qb, s) -> qb.autoGenerateSynonymsPhraseQuery(Booleans.parseBoolean(s)));
        appliers.put("fuzzy_transpositions", (qb, s) -> qb.fuzzyTranspositions(Booleans.parseBoolean(s)));
        BUILDER_APPLIERS = Collections.unmodifiableMap(appliers);
    }

    private final String query;
    private final Map<String, Float> fields;
    private StringQueryPredicate predicate;
    private final Map<String, String> options;

    // dedicated constructor for QueryTranslator
    public QueryStringQuery(Location location, String query, String fieldName) {
        this(location, query, Collections.singletonMap(fieldName, Float.valueOf(1.0f)), null);
    }

    public QueryStringQuery(Location location, String query, Map<String, Float> fields, StringQueryPredicate predicate) {
        super(location);
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
