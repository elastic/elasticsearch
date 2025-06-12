/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.MultiFieldMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ZeroTermsQueryOption;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.MultiFieldMatchQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.MultiFieldMatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MultiFieldMatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MultiFieldMatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MultiFieldMatchQueryBuilder.ZERO_TERMS_QUERY_FIELD;

public class MultiMatchQuery extends Query {

    private static final Map<String, BiConsumer<MultiFieldMatchQueryBuilder, Object>> BUILDER_APPLIERS;

    static {
        BUILDER_APPLIERS = Map.ofEntries(
            entry(BOOST_FIELD.getPreferredName(), (qb, obj) -> qb.boost((Float) obj)),
            entry(
                ZERO_TERMS_QUERY_FIELD.getPreferredName(),
                (qb, obj) -> qb.zeroTermsQuery(ZeroTermsQueryOption.readFromString((String) obj))
            ),
            entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), (qb, obj) -> qb.autoGenerateSynonymsPhraseQuery((Boolean) obj)),
            entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), (qb, obj) -> qb.minimumShouldMatch((String) obj)),
            entry(OPERATOR_FIELD.getPreferredName(), (qb, obj) -> qb.operator(Operator.fromString((String) obj)))
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
        // TODO: create a new builder, group fields by analyzer (combined_fields query), separate groups are combined with dis_max query.
        // TODO: needs to happen on shard level.

        final MultiFieldMatchQueryBuilder queryBuilder = QueryBuilders.multiFieldMatchQuery(query);
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
