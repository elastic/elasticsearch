/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class WildcardQuery extends Query {

    private final String field, query;
    private final boolean caseInsensitive;

    public WildcardQuery(Source source, String field, String query) {
        this(source, field, query, false);
    }

    public WildcardQuery(Source source, String field, String query, boolean caseInsensitive) {
        super(source);
        this.field = field;
        this.query = query;
        this.caseInsensitive = caseInsensitive;
    }

    public String field() {
        return field;
    }

    public String query() {
        return query;
    }

    public Boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    protected QueryBuilder asBuilder() {
        /*
         * Builds WildcardQueryBuilder with simple text matching semantics for
         * all fields, including the `_index` field which insists on implementing
         * some fairly unexpected matching rules.
         *
         * Note that
         */
        return new WildcardQueryBuilder(field, query) {
            @Override
            protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) throws IOException {
                MappedFieldType fieldType = context.getFieldType(fieldName());
                MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(rewrite(), null, LoggingDeprecationHandler.INSTANCE);
                LogManager.getLogger(WildcardQuery.class).error("ADSFA special query {}", fieldType);
                if (fieldType instanceof IndexFieldMapper.IndexFieldType) {
                    String value = value();
                    String indexName = context.getFullyQualifiedIndex().getName();
                    if (WildcardQuery.this.caseInsensitive) {
                        value = value.toLowerCase(Locale.ROOT);
                        indexName = indexName.toLowerCase(Locale.ROOT);
                    }
                    if (Regex.simpleMatch(value, indexName)) {
                        return new MatchAllDocsQuery();
                    }
                    return new MatchNoDocsQuery();
                }
                return fieldType.wildcardQuery(value(), method, WildcardQuery.this.caseInsensitive, context);
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, query, caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        WildcardQuery other = (WildcardQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(query, other.query)
            && Objects.equals(caseInsensitive, other.caseInsensitive);
    }

    @Override
    protected String innerToString() {
        return field + ":" + query;
    }
}
