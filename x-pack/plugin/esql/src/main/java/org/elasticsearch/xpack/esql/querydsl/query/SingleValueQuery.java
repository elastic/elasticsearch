/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.SourceUtils.readSource;
import static org.elasticsearch.xpack.esql.core.util.SourceUtils.writeSource;

/**
 * Lucene query that wraps another query and only selects documents that match
 * the wrapped query <strong>and</strong> have a single field value.
 * <p>
 *     This allows us to wrap regular lucene queries to have ESQL style semantics
 *     which will allow us to continue to push expressions to Lucene.
 * </p>
 * <p>
 *     We could have chosen not to wrap the lucene query and instead double check
 *     the results after they are loaded. That could be faster in some cases, but
 *     for now we're going to always wrap so we can always push. When we find cases
 *     where double checking is better we'll try that.
 * </p>
 */
public class SingleValueQuery extends Query {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "esql_single_value",
        Builder::new
    );

    private final Query next;
    private final String field;

    public SingleValueQuery(Query next, String field) {
        super(next.source());
        this.next = next;
        this.field = field;
    }

    @Override
    public Builder asBuilder() {
        return new Builder(next.asBuilder(), field, next.source());
    }

    @Override
    protected String innerToString() {
        return next.toString();
    }

    @Override
    public SingleValueQuery negate(Source source) {
        return new SingleValueQuery(next.negate(source), field);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass() || false == super.equals(o)) {
            return false;
        }
        SingleValueQuery other = (SingleValueQuery) o;
        return Objects.equals(next, other.next) && Objects.equals(field, other.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), next, field);
    }

    public static class Builder extends AbstractQueryBuilder<Builder> {
        private final QueryBuilder next;
        private final String field;
        private final Source source;

        Builder(QueryBuilder next, String field, Source source) {
            this.next = next;
            this.field = field;
            this.source = source;
        }

        Builder(StreamInput in) throws IOException {
            super(in);
            this.next = in.readNamedWriteable(QueryBuilder.class);
            this.field = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.source = readSource(in);
            } else {
                this.source = Source.EMPTY;

            }
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(next);
            out.writeString(field);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                writeSource(out, source);
            }
        }

        public QueryBuilder next() {
            return next;
        }

        public String field() {
            return field;
        }

        public Source source() {
            return source;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(ENTRY.name);
            builder.field("field", field);
            builder.field("next", next, params);
            builder.field("source", source.toString());
            builder.endObject();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X; // the first version of ESQL
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) throws IOException {
            MappedFieldType ft = context.getFieldType(field);
            if (ft == null) {
                return new MatchNoDocsQuery("missing field [" + field + "]");
            }
            SingleValueMatchQuery singleValueQuery = new SingleValueMatchQuery(
                context.getForField(ft, MappedFieldType.FielddataOperation.SEARCH),
                new Warnings(source)
            );
            org.apache.lucene.search.Query rewrite = singleValueQuery.rewrite(context.searcher());
            if (rewrite instanceof MatchAllDocsQuery) {
                // nothing to filter
                return next.toQuery(context);
            }
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(next.toQuery(context), BooleanClause.Occur.FILTER);
            builder.add(rewrite, BooleanClause.Occur.FILTER);
            return builder.build();
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            QueryBuilder rewritten = next.rewrite(queryRewriteContext);
            if (rewritten instanceof MatchNoneQueryBuilder) {
                return rewritten;
            }
            if (rewritten == next) {
                return this;
            }
            return new Builder(rewritten, field, source);
        }

        @Override
        protected boolean doEquals(Builder other) {
            return next.equals(other.next) && field.equals(other.field);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(next, field);
        }
    }

}
