/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public class MultiMatchQuery extends LeafQuery {

    private static final Map<String, BiConsumer<MultiMatchQueryBuilder, String>> BUILDER_APPLIERS;

    static {
        HashMap<String, BiConsumer<MultiMatchQueryBuilder, String>> appliers = new HashMap<>(14);
        // TODO: it'd be great if these could be constants instead of Strings, needs a core change to make the fields public first
        appliers.put("slop", (qb, s) -> qb.slop(Integer.valueOf(s)));
        // TODO: add zero terms query support, I'm not sure the best way to parse it yet...
        // appliers.put("zero_terms_query", (qb, s) -> qb.zeroTermsQuery(s));
        appliers.put("lenient", (qb, s) -> qb.lenient(Booleans.parseBoolean(s)));
        appliers.put("cutoff_frequency", (qb, s) -> qb.cutoffFrequency(Float.valueOf(s)));
        appliers.put("tie_breaker", (qb, s) -> qb.tieBreaker(Float.valueOf(s)));
        appliers.put("fuzzy_rewrite", (qb, s) -> qb.fuzzyRewrite(s));
        appliers.put("minimum_should_match", (qb, s) -> qb.minimumShouldMatch(s));
        appliers.put("operator", (qb, s) -> qb.operator(Operator.fromString(s)));
        appliers.put("max_expansions", (qb, s) -> qb.maxExpansions(Integer.valueOf(s)));
        appliers.put("prefix_length", (qb, s) -> qb.prefixLength(Integer.valueOf(s)));
        appliers.put("analyzer", (qb, s) -> qb.analyzer(s));
        appliers.put("type", (qb, s) -> qb.type(s));
        appliers.put("auto_generate_synonyms_phrase_query", (qb, s) -> qb.autoGenerateSynonymsPhraseQuery(Booleans.parseBoolean(s)));
        appliers.put("fuzzy_transpositions", (qb, s) -> qb.fuzzyTranspositions(Booleans.parseBoolean(s)));
        BUILDER_APPLIERS = Collections.unmodifiableMap(appliers);
    }

    private final String query;
    private final Map<String, Float> fields;
    private final Map<String, String> options;
    private final MultiMatchQueryPredicate predicate;

    public MultiMatchQuery(Source source, String query, Map<String, Float> fields, MultiMatchQueryPredicate predicate) {
        super(source);
        this.query = query;
        this.fields = fields;
        this.predicate = predicate;
        this.options = predicate.optionMap();
    }

    @Override
    public QueryBuilder asBuilder() {
        final MultiMatchQueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(query);
        queryBuilder.fields(fields);
        queryBuilder.analyzer(predicate.analyzer());
        options.forEach((k, v) -> {
            if (BUILDER_APPLIERS.containsKey(k)) {
                BUILDER_APPLIERS.get(k).accept(queryBuilder, v);
            } else {
                throw new IllegalArgumentException("illegal multi_match option [" + k + "]");
            }
        });
        return queryBuilder;
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

        MultiMatchQuery other = (MultiMatchQuery) obj;
        return Objects.equals(query, other.query)
                && Objects.equals(fields, other.fields)
                && Objects.equals(predicate, other.predicate);
    }

    @Override
    protected String innerToString() {
        return fields + ":" + query;
    }
}
