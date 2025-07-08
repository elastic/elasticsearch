/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Map.entry;

public class KqlQuery extends Query {

    private static final Map<String, BiConsumer<KqlQueryBuilder, String>> BUILDER_APPLIERS = Map.ofEntries(
        entry(KqlQueryBuilder.TIME_ZONE_FIELD.getPreferredName(), KqlQueryBuilder::timeZone),
        entry(KqlQueryBuilder.DEFAULT_FIELD_FIELD.getPreferredName(), KqlQueryBuilder::defaultField),
        entry(KqlQueryBuilder.CASE_INSENSITIVE_FIELD.getPreferredName(), (qb, s) -> qb.caseInsensitive(Booleans.parseBoolean(s)))
    );

    private final String query;

    private final Map<String, String> options;

    // dedicated constructor for QueryTranslator
    public KqlQuery(Source source, String query) {
        this(source, query, null);
    }

    public KqlQuery(Source source, String query, Map<String, String> options) {
        super(source);
        this.query = query;
        this.options = options == null ? Collections.emptyMap() : options;
    }

    @Override
    protected QueryBuilder asBuilder() {
        final KqlQueryBuilder queryBuilder = new KqlQueryBuilder(query);
        options.forEach((k, v) -> {
            if (BUILDER_APPLIERS.containsKey(k)) {
                BUILDER_APPLIERS.get(k).accept(queryBuilder, v);
            } else {
                throw new IllegalArgumentException("illegal kql query option [" + k + "]");
            }
        });
        return queryBuilder;
    }

    public String query() {
        return query;
    }

    public Map<String, String> options() {
        return options;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }

        KqlQuery other = (KqlQuery) obj;
        return Objects.equals(query, other.query) && Objects.equals(options, other.options);
    }

    @Override
    protected String innerToString() {
        return query;
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
