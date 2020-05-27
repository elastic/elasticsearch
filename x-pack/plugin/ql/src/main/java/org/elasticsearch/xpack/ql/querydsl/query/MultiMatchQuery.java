/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Map.entry;

public class MultiMatchQuery extends LeafQuery {

    private static final Map<String, BiConsumer<MultiMatchQueryBuilder, String>> BUILDER_APPLIERS;

    static {
        // TODO: it'd be great if these could be constants instead of Strings, needs a core change to make the fields public first
        BUILDER_APPLIERS = Map.ofEntries(
                entry("slop", (qb, s) -> qb.slop(Integer.valueOf(s))),
                // TODO: add zero terms query support, I'm not sure the best way to parse it yet...
                // appliers.put("zero_terms_query", (qb, s) -> qb.zeroTermsQuery(s));
                entry("analyzer", MultiMatchQueryBuilder::analyzer),
                entry("auto_generate_synonyms_phrase_query", (qb, s) -> qb.autoGenerateSynonymsPhraseQuery(Booleans.parseBoolean(s))),
                entry("fuzziness", (qb, s) -> qb.fuzziness(Fuzziness.fromString(s))),
                entry("fuzzy_rewrite", MultiMatchQueryBuilder::fuzzyRewrite),
                entry("fuzzy_transpositions", (qb, s) -> qb.fuzzyTranspositions(Booleans.parseBoolean(s))),
                entry("lenient", (qb, s) -> qb.lenient(Booleans.parseBoolean(s))),
                entry("max_expansions", (qb, s) -> qb.maxExpansions(Integer.valueOf(s))),
                entry("minimum_should_match", MultiMatchQueryBuilder::minimumShouldMatch),
                entry("operator", (qb, s) -> qb.operator(Operator.fromString(s))),
                entry("prefix_length", (qb, s) -> qb.prefixLength(Integer.valueOf(s))),
                entry("tie_breaker", (qb, s) -> qb.tieBreaker(Float.valueOf(s))),
                entry("type", MultiMatchQueryBuilder::type));
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
