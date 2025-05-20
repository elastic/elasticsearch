/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZINESS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.PREFIX_LENGTH_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.SLOP_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.TIE_BREAKER_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.TYPE_FIELD;

public class MultiMatchQuery extends Query {

    private static final Map<String, BiConsumer<MultiMatchQueryBuilder, Object>> BUILDER_APPLIERS;

    static {
        BUILDER_APPLIERS = Map.ofEntries(
            entry(BOOST_FIELD.getPreferredName(), (qb, obj) -> qb.boost((Float) obj)),
            entry(SLOP_FIELD.getPreferredName(), (qb, obj) -> qb.slop((Integer) obj)),
            // TODO: add zero terms query support, I'm not sure the best way to parse it yet...
            // appliers.put("zero_terms_query", (qb, s) -> qb.zeroTermsQuery(s));
            entry(ANALYZER_FIELD.getPreferredName(), (qb, obj) -> qb.analyzer((String) obj)),
            entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), (qb, obj) -> qb.autoGenerateSynonymsPhraseQuery((Boolean) obj)),
            entry(FUZZINESS_FIELD.getPreferredName(), (qb, obj) -> qb.fuzziness(Fuzziness.fromString((String) obj))),
            entry(FUZZY_REWRITE_FIELD.getPreferredName(), (qb, obj) -> qb.fuzzyRewrite((String) obj)),
            entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), (qb, obj) -> qb.fuzzyTranspositions((Boolean) obj)),
            entry(LENIENT_FIELD.getPreferredName(), (qb, obj) -> qb.lenient((Boolean) obj)),
            entry(MAX_EXPANSIONS_FIELD.getPreferredName(), (qb, obj) -> qb.maxExpansions((Integer) obj)),
            entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), (qb, obj) -> qb.minimumShouldMatch((String) obj)),
            entry(OPERATOR_FIELD.getPreferredName(), (qb, obj) -> qb.operator(Operator.fromString((String) obj))),
            entry(PREFIX_LENGTH_FIELD.getPreferredName(), (qb, obj) -> qb.prefixLength((Integer) obj)),
            entry(TIE_BREAKER_FIELD.getPreferredName(), (qb, obj) -> qb.tieBreaker((Float) obj)),
            entry(TYPE_FIELD.getPreferredName(), (qb, obj) -> qb.type((String) obj))
        );
    }

    private final String query;
    private final Map<String, Float> fields;
    private final Map<String, Object> options;

    public MultiMatchQuery(Source source, String query, Map<String, Float> fields, Map<String, Object> options) {
        super(source);
        this.query = query;
        this.fields = fields;
        this.options = options;
    }

    @Override
    protected QueryBuilder asBuilder() {
        final MultiMatchQueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(query);
        queryBuilder.fields(fields);
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
        return Objects.hash(query, fields, options);
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
        return Objects.equals(query, other.query) && Objects.equals(fields, other.fields) && Objects.equals(options, other.options);
    }

    @Override
    protected String innerToString() {
        // Use a TreeMap so we get the fields in a predictable order.
        return new TreeMap<>(fields) + ":" + query;
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
