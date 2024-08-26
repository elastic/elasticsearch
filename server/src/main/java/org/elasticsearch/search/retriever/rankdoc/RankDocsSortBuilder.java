/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortFieldAndFormat;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Builds a {@code RankDocsSortField} that sorts documents by their rank as computed through the {@code RankDocsRetrieverBuilder}.
 */
public class RankDocsSortBuilder extends SortBuilder<RankDocsSortBuilder> {
    public static final String NAME = "rank_docs_sort";

    private RankDoc[] rankDocs;

    public RankDocsSortBuilder(RankDoc[] rankDocs) {
        this.rankDocs = rankDocs;
    }

    public RankDocsSortBuilder(StreamInput in) throws IOException {
        this.rankDocs = in.readArray(c -> c.readNamedWriteable(RankDoc.class), RankDoc[]::new);
    }

    public RankDocsSortBuilder(RankDocsSortBuilder original) {
        this.rankDocs = original.rankDocs;
    }

    public RankDocsSortBuilder rankDocs(RankDoc[] rankDocs) {
        this.rankDocs = rankDocs;
        return this;
    }

    public RankDoc[] rankDocs() {
        return this.rankDocs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeNamedWriteable, rankDocs);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SortBuilder<?> rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    @Override
    protected SortFieldAndFormat build(SearchExecutionContext context) throws IOException {
        RankDoc[] shardRankDocs = Arrays.stream(rankDocs)
            .filter(r -> r.shardIndex == context.getShardRequestIndex())
            .toArray(RankDoc[]::new);
        return new SortFieldAndFormat(new RankDocsSortField(shardRankDocs), DocValueFormat.RAW);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RANK_DOCS_RETRIEVER;
    }

    @Override
    public BucketedSort buildBucketedSort(SearchExecutionContext context, BigArrays bigArrays, int bucketSize, BucketedSort.ExtraData extra)
        throws IOException {
        throw new UnsupportedOperationException("buildBucketedSort() is not supported for " + this.getClass());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("toXContent() is not supported for " + this.getClass());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RankDocsSortBuilder that = (RankDocsSortBuilder) obj;
        return Arrays.equals(rankDocs, that.rankDocs) && this.order.equals(that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(this.rankDocs), this.order);
    }
}
