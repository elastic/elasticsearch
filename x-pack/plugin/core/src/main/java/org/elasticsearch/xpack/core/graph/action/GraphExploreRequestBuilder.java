/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.graph.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;
import org.elasticsearch.protocol.xpack.graph.Hop;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;

/**
 * Creates a new {@link GraphExploreRequestBuilder}
 *
 * @see GraphExploreRequest
 */
public class GraphExploreRequestBuilder extends ActionRequestBuilder<GraphExploreRequest, GraphExploreResponse> {

    public GraphExploreRequestBuilder(ElasticsearchClient client, GraphExploreAction action) {
        super(client, action, new GraphExploreRequest());
    }

    public GraphExploreRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public GraphExploreRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the action will be executed on.
     */
    public GraphExploreRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the action will be executed on.
     */
    public GraphExploreRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Optional choice of single-value field on which to diversify sampled
     * search results
     */
    public GraphExploreRequestBuilder sampleDiversityField(String fieldName) {
        request.sampleDiversityField(fieldName);
        return this;
    }

    public String sampleDiversityField() {
        return request.sampleDiversityField();
    }

    /**
     * Optional number of permitted docs with same value in sampled search
     * results. Must also declare which field using sampleDiversityField
     */
    public GraphExploreRequestBuilder maxDocsPerDiversityValue(int max) {
        request.maxDocsPerDiversityValue(max);
        return this;
    }

    public int maxDocsPerDiversityValue() {
        return request.maxDocsPerDiversityValue();
    }


    /**
     * An optional timeout to control how long the graph exploration is allowed
     * to take.
     */
    public GraphExploreRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * An optional timeout to control how long the graph exploration is allowed
     * to take.
     */
    public GraphExploreRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Add a stage in the graph exploration. Each hop represents a stage of
     * querying elasticsearch to identify terms which can then be connnected
     * to other terms in a subsequent hop.
     * @param guidingQuery optional choice of query which influences which documents
     * are considered in this stage
     * @return a {@link Hop} object that holds settings for a stage in the graph exploration
     */
    public Hop createNextHop(@Nullable QueryBuilder guidingQuery) {
        return request.createNextHop(guidingQuery);
    }

    /**
     * Controls the choice of algorithm used to select interesting terms. The default
     * value is true which means terms are selected based on significance (see the {@link SignificantTerms}
     * aggregation) rather than popularity (using the {@link TermsAggregator}).
     * @param value true if the significant_terms algorithm should be used.
     */
    public GraphExploreRequestBuilder useSignificance(boolean value) {
        request.useSignificance(value);
        return this;
    }


    /**
     * The number of top-matching documents that are considered during each hop (default is
     * {@link SamplerAggregationBuilder#DEFAULT_SHARD_SAMPLE_SIZE}
     * Very small values (less than 50) may not provide sufficient weight-of-evidence to identify
     * significant connections between terms.
     * <p> Very large values (many thousands) are not recommended with loosely defined queries (fuzzy queries or
     *  those with many OR clauses).
     *  This is because any useful signals in the best documents are diluted with irrelevant noise from low-quality matches.
     *  Performance is also typically better with smaller samples as there are less look-ups required for background frequencies
     *  of terms found in the documents
     * </p>
     *
     * @param maxNumberOfDocsPerHop the shard-level sample size in documents
     */
    public GraphExploreRequestBuilder sampleSize(int maxNumberOfDocsPerHop) {
        request.sampleSize(maxNumberOfDocsPerHop);
        return this;
    }

}
