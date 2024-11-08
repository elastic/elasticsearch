/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.RRF_QUERY_REWRITE;

public class RankDocsQueryBuilder extends AbstractQueryBuilder<RankDocsQueryBuilder> {

    public static final String NAME = "rank_docs_query";

    private final RankDoc[] rankDocs;
    private final QueryBuilder[] queryBuilders;
    private final boolean onlyRankDocs;

    public RankDocsQueryBuilder(RankDoc[] rankDocs, QueryBuilder[] queryBuilders, boolean onlyRankDocs) {
        this.rankDocs = rankDocs;
        this.queryBuilders = queryBuilders;
        this.onlyRankDocs = onlyRankDocs;
    }

    public RankDocsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.rankDocs = in.readArray(c -> c.readNamedWriteable(RankDoc.class), RankDoc[]::new);
        if (in.getTransportVersion().onOrAfter(RRF_QUERY_REWRITE)) {
            this.queryBuilders = in.readOptionalArray(c -> c.readNamedWriteable(QueryBuilder.class), QueryBuilder[]::new);
            this.onlyRankDocs = in.readBoolean();
        } else {
            this.queryBuilders = null;
            this.onlyRankDocs = false;
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryBuilders != null) {
            QueryBuilder[] newQueryBuilders = new QueryBuilder[queryBuilders.length];
            boolean changed = false;
            for (int i = 0; i < newQueryBuilders.length; i++) {
                newQueryBuilders[i] = queryBuilders[i].rewrite(queryRewriteContext);
                changed |= newQueryBuilders[i] != queryBuilders[i];
            }
            if (changed) {
                return new RankDocsQueryBuilder(rankDocs, newQueryBuilders, onlyRankDocs);
            }
        }
        return super.doRewrite(queryRewriteContext);
    }

    RankDoc[] rankDocs() {
        return rankDocs;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeNamedWriteable, rankDocs);
        if (out.getTransportVersion().onOrAfter(RRF_QUERY_REWRITE)) {
            out.writeOptionalArray(StreamOutput::writeNamedWriteable, queryBuilders);
            out.writeBoolean(onlyRankDocs);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        RankDoc[] shardRankDocs = Arrays.stream(rankDocs)
            .filter(r -> r.shardIndex == context.getShardRequestIndex())
            .toArray(RankDoc[]::new);
        IndexReader reader = context.getIndexReader();
        final Query[] queries;
        final String[] queryNames;
        if (queryBuilders != null) {
            queries = new Query[queryBuilders.length];
            queryNames = new String[queryBuilders.length];
            for (int i = 0; i < queryBuilders.length; i++) {
                queries[i] = queryBuilders[i].toQuery(context);
                queryNames[i] = queryBuilders[i].queryName();
            }
        } else {
            queries = new Query[0];
            queryNames = Strings.EMPTY_ARRAY;
        }
        return new RankDocsQuery(reader, shardRankDocs, queries, queryNames, onlyRankDocs);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("rank_docs");
        for (RankDoc doc : rankDocs) {
            builder.startObject();
            doc.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    protected boolean doEquals(RankDocsQueryBuilder other) {
        return Arrays.equals(rankDocs, other.rankDocs)
            && Arrays.equals(queryBuilders, other.queryBuilders)
            && onlyRankDocs == other.onlyRankDocs;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(Arrays.hashCode(rankDocs), Arrays.hashCode(queryBuilders), onlyRankDocs);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RANK_DOCS_RETRIEVER;
    }
}
