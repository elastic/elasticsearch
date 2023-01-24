/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rerank;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class RerankQueryBuilder extends AbstractQueryBuilder<RerankQueryBuilder> {

    public static final String NAME = "rerank";

    private final List<QueryBuilder> queryBuilders;
    private QueryBuilder compoundQueryBuilder;

    public RerankQueryBuilder() {
        queryBuilders = new ArrayList<>();
        compoundQueryBuilder = null;
    }

    public RerankQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilders = readQueries(in);
        compoundQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeTo(out);
        out.writeList(queryBuilders);
        out.writeOptionalNamedWriteable(compoundQueryBuilder);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.array("queries", queryBuilders);
        if (compoundQueryBuilder != null) {
            builder.field("compound", compoundQueryBuilder);
        }
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_7_0;
    }

    @Override
    public String getName() {
        return getWriteableName();
    }

    public RerankQueryBuilder addQuery(QueryBuilder queryBuilder) {
        queryBuilders.add(queryBuilder);
        return this;
    }

    @Override
    public QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        RerankQueryBuilder rerankQueryBuilder = new RerankQueryBuilder();;
        BoolQueryBuilder compoundQueryBuilder = new BoolQueryBuilder();
        boolean changed = false;
        for (QueryBuilder queryBuilder : queryBuilders) {
            QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(queryRewriteContext);
            rerankQueryBuilder.addQuery(rewrittenQueryBuilder);
            compoundQueryBuilder.should(rewrittenQueryBuilder);
            changed |= rewrittenQueryBuilder != queryBuilder;
        }
        this.compoundQueryBuilder = compoundQueryBuilder.rewrite(queryRewriteContext);
        changed |= this.compoundQueryBuilder != compoundQueryBuilder;
        if (changed) {
            rerankQueryBuilder.compoundQueryBuilder = this.compoundQueryBuilder;
            return rerankQueryBuilder;
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return compoundQueryBuilder.toQuery(context);
    }

    public Iterator<QueryBuilder> iterator() {
        return queryBuilders.iterator();
    }

    @Override
    public boolean doEquals(RerankQueryBuilder o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        return Objects.equals(queryBuilders, o.queryBuilders);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.hashCode(), queryBuilders);
    }
}
