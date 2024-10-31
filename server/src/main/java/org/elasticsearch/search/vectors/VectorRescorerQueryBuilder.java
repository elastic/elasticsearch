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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

// WIP - Perform a query on the rewrite phase, then do an exact query with a filter
public class VectorRescorerQueryBuilder extends AbstractQueryBuilder<VectorRescorerQueryBuilder> {

    private final QueryBuilder knnQueryBuilder;

    public VectorRescorerQueryBuilder(QueryBuilder knnQueryBuilder) {
        this.knnQueryBuilder = knnQueryBuilder;
    }

    @Override
    protected QueryBuilder doIndexMetadataRewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder rewrittenQueryBuilder = knnQueryBuilder.rewrite(context);
        if (rewrittenQueryBuilder != knnQueryBuilder) {
            return new VectorRescorerQueryBuilder(rewrittenQueryBuilder);
        }

        // Query query = knnQueryBuilder.toQuery(context);
        context.registerAsyncAction((client, listener) -> {
            String[] indices = Arrays.stream(context.getResolvedIndices().getConcreteLocalIndices())
                .map(Index::getName)
                .toArray(String[]::new);
            client.prepareSearch(indices)
                .setPreference(Preference.ONLY_LOCAL.type())
                .setQuery(knnQueryBuilder)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {

                    }

                    @Override
                    public void onFailure(Exception e) {

                    }
                });
        });
        // client.execute(new ESKnnFloatVectorQuery("field", new float[0], 0, 0, query), listener);
        // });

        return this; // super.doIndexMetadataRewrite(context);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {

    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return null;
    }

    @Override
    protected boolean doEquals(VectorRescorerQueryBuilder other) {
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
}
