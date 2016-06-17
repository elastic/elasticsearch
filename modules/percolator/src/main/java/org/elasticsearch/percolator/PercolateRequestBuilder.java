/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.percolator;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.Map;

/**
 * A builder the easy to use of defining a percolate request.
 *
 * @deprecated Instead use search API with {@link PercolateQueryBuilder}
 */
@Deprecated
public class PercolateRequestBuilder extends ActionRequestBuilder<PercolateRequest, PercolateResponse, PercolateRequestBuilder> {

    private PercolateSourceBuilder sourceBuilder;

    public PercolateRequestBuilder(ElasticsearchClient client, PercolateAction action) {
        super(client, action, new PercolateRequest());
    }

    public PercolateRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public PercolateRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Sets the type of the document to percolate. This is important as it selects the mapping to be used to parse
     * the document.
     */
    public PercolateRequestBuilder setDocumentType(String type) {
        request.documentType(type);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public PercolateRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * List of routing values to control the shards the search will be executed on.
     */
    public PercolateRequestBuilder setRouting(String... routings) {
        request.routing(Strings.arrayToCommaDelimitedString(routings));
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public PercolateRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Enables percolating an existing document. Instead of specifying the source of the document to percolate, define
     * a get request that will fetch a document and use its source.
     */
    public PercolateRequestBuilder setGetRequest(GetRequest getRequest) {
        request.getRequest(getRequest);
        return this;
    }

    /**
     * Whether only to return total count and don't keep track of the matches (Count percolation).
     */
    public PercolateRequestBuilder setOnlyCount(boolean onlyCount) {
        request.onlyCount(onlyCount);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#setSize(int)}}
     */
    public PercolateRequestBuilder setSize(int size) {
        sourceBuilder().setSize(size);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#setSort(boolean)}}
     */
    public PercolateRequestBuilder setSortByScore(boolean sort) {
        sourceBuilder().setSort(sort);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#addSort(SortBuilder)}
     */
    public PercolateRequestBuilder addSort(SortBuilder<?> sort) {
        sourceBuilder().addSort(sort);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#setSort(boolean)}}
     */
    public PercolateRequestBuilder setScore(boolean score) {
        sourceBuilder().setTrackScores(score);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#setDoc(PercolateSourceBuilder.DocBuilder)}
     */
    public PercolateRequestBuilder setPercolateDoc(PercolateSourceBuilder.DocBuilder docBuilder) {
        sourceBuilder().setDoc(docBuilder);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#setQueryBuilder(QueryBuilder)}
     */
    public PercolateRequestBuilder setPercolateQuery(QueryBuilder queryBuilder) {
        sourceBuilder().setQueryBuilder(queryBuilder);
        return this;
    }

    /**
     * Delegates to {@link PercolateSourceBuilder#setHighlightBuilder(HighlightBuilder)}
     */
    public PercolateRequestBuilder setHighlightBuilder(HighlightBuilder highlightBuilder) {
        sourceBuilder().setHighlightBuilder(highlightBuilder);
        return this;
    }

    /**
     * Delegates to
     * {@link PercolateSourceBuilder#addAggregation(AggregationBuilder)}
     */
    public PercolateRequestBuilder addAggregation(AggregationBuilder aggregationBuilder) {
        sourceBuilder().addAggregation(aggregationBuilder);
        return this;
    }

    /**
     * Delegates to
     * {@link PercolateSourceBuilder#addAggregation(PipelineAggregationBuilder)}
     */
    public PercolateRequestBuilder addAggregation(PipelineAggregationBuilder aggregationBuilder) {
        sourceBuilder().addAggregation(aggregationBuilder);
        return this;
    }

    /**
     * Sets the percolate request definition directly on the request. This will
     * overwrite any definitions set by any of the delegate methods.
     */
    public PercolateRequestBuilder setSource(PercolateSourceBuilder source) {
        sourceBuilder = source;
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(Map<String, Object> source) {
        request.source(source);
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(Map<String, Object> source, XContentType contentType) {
        request.source(source, contentType);
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(BytesReference source) {
        request.source(source);
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    /**
     * Raw variant of {@link #setSource(PercolateSourceBuilder)}
     */
    public PercolateRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    private PercolateSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new PercolateSourceBuilder();
        }
        return sourceBuilder;
    }

    @Override
    public PercolateRequest request() {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }
        return request;
    }

    @Override
    protected PercolateRequest beforeExecute(PercolateRequest request) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }
        return request;
    }
}
