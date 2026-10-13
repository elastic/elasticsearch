/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.util.Objects;

public class ExistsQuery extends Query {

    private final String name;

    public ExistsQuery(Source source, String name) {
        super(source);
        this.name = name;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new MappedOnlyExistsQueryBuilder(name);
    }

    /**
     * An {@code exists} query that matches nothing on shards where {@link SearchContextStats#extractableFieldType}
     * treats the field as missing. It keeps the {@code exists} wire format, so a node that receives it over the
     * wire runs a plain {@link ExistsQueryBuilder}.
     */
    private static class MappedOnlyExistsQueryBuilder extends ExistsQueryBuilder {
        MappedOnlyExistsQueryBuilder(String fieldName) {
            super(fieldName);
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) {
            MappedFieldType ft = SearchContextStats.extractableFieldType(context, fieldName());
            if (ft == null) {
                return new MatchNoDocsQuery("missing field [" + fieldName() + "]");
            }
            return new ConstantScoreQuery(ft.existsQuery(context));
        }
    }

    @Override
    protected String innerToString() {
        return name;
    }

    @Override
    public boolean containsPlan() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExistsQuery other = (ExistsQuery) obj;
        return Objects.equals(name, other.name);
    }
}
