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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder to create the percolate request body.
 *
 * @deprecated Instead use search API with {@link PercolateQueryBuilder}
 */
@Deprecated
public class PercolateSourceBuilder extends ToXContentToBytes {

    private DocBuilder docBuilder;
    private QueryBuilder queryBuilder;
    private Integer size;
    private List<SortBuilder<?>> sorts;
    private Boolean trackScores;
    private HighlightBuilder highlightBuilder;
    private List<AggregationBuilder> aggregationBuilders;
    private List<PipelineAggregationBuilder> pipelineAggregationBuilders;

    /**
     * Sets the document to run the percolate queries against.
     */
    public PercolateSourceBuilder setDoc(DocBuilder docBuilder) {
        this.docBuilder = docBuilder;
        return this;
    }

    /**
     * Sets a query to reduce the number of percolate queries to be evaluated and score the queries that match based
     * on this query.
     */
    public PercolateSourceBuilder setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    /**
     * Limits the maximum number of percolate query matches to be returned.
     */
    public PercolateSourceBuilder setSize(int size) {
        this.size = size;
        return this;
    }

    /**
     * Similar as {@link #setTrackScores(boolean)}, but whether to sort by the score descending.
     */
    public PercolateSourceBuilder setSort(boolean sort) {
        if (sort) {
            addSort(new ScoreSortBuilder());
        } else {
            this.sorts = null;
        }
        return this;
    }

    /**
     * Adds a sort builder. Only sorting by score desc is supported.
     *
     * By default the matching percolator queries are returned in an undefined order.
     */
    public PercolateSourceBuilder addSort(SortBuilder<?> sort) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    /**
     * Whether to compute a score for each match and include it in the response. The score is based on
     * {@link #setQueryBuilder(QueryBuilder)}.
     */
    public PercolateSourceBuilder setTrackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Enables highlighting for the percolate document. Per matched percolate query highlight the percolate document.
     */
    public PercolateSourceBuilder setHighlightBuilder(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Add an aggregation definition.
     */
    public PercolateSourceBuilder addAggregation(AggregationBuilder aggregationBuilder) {
        if (aggregationBuilders == null) {
            aggregationBuilders = new ArrayList<>();
        }
        aggregationBuilders.add(aggregationBuilder);
        return this;
    }

    /**
     * Add an aggregation definition.
     */
    public PercolateSourceBuilder addAggregation(PipelineAggregationBuilder aggregationBuilder) {
        if (pipelineAggregationBuilders == null) {
            pipelineAggregationBuilders = new ArrayList<>();
        }
        pipelineAggregationBuilders.add(aggregationBuilder);
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (docBuilder != null) {
            docBuilder.toXContent(builder, params);
        }
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (size != null) {
            builder.field("size", size);
        }
        if (sorts != null) {
            builder.startArray("sort");
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (trackScores != null) {
            builder.field("track_scores", trackScores);
        }
        if (highlightBuilder != null) {
            builder.field(SearchSourceBuilder.HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder);
        }
        if (aggregationBuilders != null || pipelineAggregationBuilders != null) {
            builder.field("aggregations");
            builder.startObject();
            if (aggregationBuilders != null) {
                for (AggregationBuilder aggregation : aggregationBuilders) {
                    aggregation.toXContent(builder, params);
                }
            }
            if (pipelineAggregationBuilders != null) {
                for (PipelineAggregationBuilder aggregation : pipelineAggregationBuilders) {
                    aggregation.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * @return A new {@link DocBuilder} instance.
     */
    public static DocBuilder docBuilder() {
        return new DocBuilder();
    }

    /**
     * A builder for defining the document to be percolated in various ways.
     */
    public static class DocBuilder implements ToXContent {

        private BytesReference doc;

        /**
         * Sets the document to be percolated.
         */
        public DocBuilder setDoc(BytesReference doc) {
            this.doc = doc;
            return this;
        }

        /**
         * Sets the document to be percolated.
         */
        public DocBuilder setDoc(String field, Object value) {
            Map<String, Object> values = new HashMap<>(2);
            values.put(field, value);
            setDoc(values);
            return this;
        }

        /**
         * Sets the document to be percolated.
         */
        public DocBuilder setDoc(String doc) {
            this.doc = new BytesArray(doc);
            return this;
        }

        /**
         * Sets the document to be percolated.
         */
        public DocBuilder setDoc(XContentBuilder doc) {
            this.doc = doc.bytes();
            return this;
        }

        /**
         * Sets the document to be percolated.
         */
        public DocBuilder setDoc(Map doc) {
            return setDoc(doc, Requests.CONTENT_TYPE);
        }

        @SuppressWarnings("unchecked")
        public DocBuilder setDoc(Map doc, XContentType contentType) {
            try {
                return setDoc(XContentFactory.contentBuilder(contentType).map(doc));
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + doc + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.rawField("doc", doc);
        }
    }

}
