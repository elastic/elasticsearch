/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.core.Nullable;
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
     * The type of {@code field} on this shard, or {@code null} if ES|QL treats the field as missing here.
     * A dotted name under a {@code flattened} root resolves to a keyed sub-field type even though the shard
     * has no mapping for it, and a sub-field of a {@code nested} object resolves to its own type. Field caps
     * and field extraction treat both as missing, so pushed-down queries have to as well: otherwise they
     * reject wildcard/regexp queries or match documents whose extracted value is {@code null}, for example
     * through a nested object's {@code include_in_root} copy.
     */
    @Nullable
    public static MappedFieldType mappedFieldType(SearchExecutionContext context, String field) {
        MappedFieldType ft = context.getFieldType(field);
        if (ft == null || SearchContextStats.isExtractableMappedField(context, field) == false) {
            return null;
        }
        return ft;
    }

    /**
     * An {@code exists} query that matches nothing on shards where {@link #mappedFieldType} treats the field as missing.
     * It keeps the {@code exists} wire name, so it deserializes as a plain {@link ExistsQueryBuilder}. That is safe because
     * it is only serialized for can-match, which never calls {@link #doToQuery}. Adding fields requires its own registered name
     * and a transport version.
     */
    private static class MappedOnlyExistsQueryBuilder extends ExistsQueryBuilder {
        MappedOnlyExistsQueryBuilder(String fieldName) {
            super(fieldName);
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) {
            MappedFieldType ft = mappedFieldType(context, fieldName());
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
