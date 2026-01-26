/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.OVERSAMPLE_LIMIT;

/**
 * A query that performs kNN search using Lucene's {@link org.apache.lucene.search.KnnFloatVectorQuery} or
 * {@link org.apache.lucene.search.KnnByteVectorQuery}.
 */
public class DFSKnnVectorQueryBuilder extends AbstractQueryBuilder<DFSKnnVectorQueryBuilder> {

    private final KnnVectorQueryBuilder knnVectorQueryBuilder;
    private final RescoreVectorBuilder rescoreVectorBuilder;
    private final Integer k;

    private static final String NAME = "dfs_knn";

    public DFSKnnVectorQueryBuilder(String field, VectorData vector, Integer k, Integer numCands, Float visitPercentage, RescoreVectorBuilder rescoreVectorBuilder, Float similarity) {
        this.knnVectorQueryBuilder = new KnnVectorQueryBuilder(
            field,
            vector,
            rescoreVectorBuilder == null ? k : applyRescoringOversample(rescoreVectorBuilder, k),
            rescoreVectorBuilder == null ? numCands: applyRescoringOversample(rescoreVectorBuilder, numCands),
            visitPercentage,
            null,
            similarity);
        this.k = k;
        this.rescoreVectorBuilder = rescoreVectorBuilder;
    }

    private DFSKnnVectorQueryBuilder(KnnVectorQueryBuilder knnVectorQueryBuilder, int k, RescoreVectorBuilder rescoreVectorBuilder) {
        this.knnVectorQueryBuilder = knnVectorQueryBuilder;
        this.k = k;
        this.rescoreVectorBuilder = rescoreVectorBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {

    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        knnVectorQueryBuilder.doXContent(builder, params);
        if (k != null) {
            builder.field("k", k);
        }
        if (rescoreVectorBuilder != null) {
            builder.field("rescore", rescoreVectorBuilder);
        }
        builder.endObject();
    }

    public RescoreVectorBuilder rescoreVectorBuilder() {
        return rescoreVectorBuilder;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return knnVectorQueryBuilder.toQuery(context);
    }

    @Override
    protected boolean doEquals(DFSKnnVectorQueryBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return "";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }

    public DFSKnnVectorQueryBuilder addFilterQueries(List<QueryBuilder> filterQueries) {
        knnVectorQueryBuilder.addFilterQueries(filterQueries);
        return this;
    }

    public DFSKnnVectorQueryBuilder addFilterQuery(QueryBuilder queryBuilder) {
        knnVectorQueryBuilder.addFilterQuery(queryBuilder);
        return this;
    }

    public String getFieldName() {
        return  knnVectorQueryBuilder.getFieldName();
    }

    public int k(){
        return k;
    }

    @Override
    public QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder inner = knnVectorQueryBuilder.doRewrite(context);
        if (inner.equals(knnVectorQueryBuilder)) {
            return this;
        }
        return new DFSKnnVectorQueryBuilder((KnnVectorQueryBuilder) inner, k, rescoreVectorBuilder);
    }

    private static int applyRescoringOversample(RescoreVectorBuilder rescoreVectorBuilder, int k) {
        if(rescoreVectorBuilder == null) {
            return k;
        }
        return Math.min((int) Math.ceil(k * rescoreVectorBuilder.oversample()), OVERSAMPLE_LIMIT);
    }
}
