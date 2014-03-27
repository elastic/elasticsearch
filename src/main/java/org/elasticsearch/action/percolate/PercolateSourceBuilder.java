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

package org.elasticsearch.action.percolate;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder to create the percolate request body.
 */
public class PercolateSourceBuilder implements ToXContent {

    private DocBuilder docBuilder;
    private QueryBuilder queryBuilder;
    private FilterBuilder filterBuilder;
    private Integer size;
    private Boolean sort;
    private List<SortBuilder> sorts;
    private Boolean trackScores;
    private HighlightBuilder highlightBuilder;
    private List<FacetBuilder> facets;
    private List<AggregationBuilder> aggregations;

    public DocBuilder percolateDocument() {
        if (docBuilder == null) {
            docBuilder = new DocBuilder();
        }
        return docBuilder;
    }

    public DocBuilder getDoc() {
        return docBuilder;
    }

    /**
     * Sets the document to run the percolate queries against.
     */
    public PercolateSourceBuilder setDoc(DocBuilder docBuilder) {
        this.docBuilder = docBuilder;
        return this;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    /**
     * Sets a query to reduce the number of percolate queries to be evaluated and score the queries that match based
     * on this query.
     */
    public PercolateSourceBuilder setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    public FilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    /**
     * Sets a filter to reduce the number of percolate queries to be evaluated.
     */
    public PercolateSourceBuilder setFilterBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
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
     */
    public PercolateSourceBuilder addSort(SortBuilder sort) {
        if (sorts == null) {
            sorts = Lists.newArrayList();
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
     * Add a facet definition.
     */
    public PercolateSourceBuilder addFacet(FacetBuilder facetBuilder) {
        if (facets == null) {
            facets = Lists.newArrayList();
        }
        facets.add(facetBuilder);
        return this;
    }

    /**
     * Add an aggregationB definition.
     */
    public PercolateSourceBuilder addAggregation(AggregationBuilder aggregationBuilder) {
        if (aggregations == null) {
            aggregations = Lists.newArrayList();
        }
        aggregations.add(aggregationBuilder);
        return this;
    }

    public BytesReference buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.bytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
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
        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        if (size != null) {
            builder.field("size", size);
        }
        if (sorts != null) {
            builder.startArray("sort");
            for (SortBuilder sort : sorts) {
                builder.startObject();
                sort.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        if (trackScores != null) {
            builder.field("track_scores", trackScores);
        }
        if (highlightBuilder != null) {
            highlightBuilder.toXContent(builder, params);
        }
        if (facets != null) {
            builder.field("facets");
            builder.startObject();
            for (FacetBuilder facet : facets) {
                facet.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (aggregations != null) {
            builder.field("aggregations");
            builder.startObject();
            for (AbstractAggregationBuilder aggregation : aggregations) {
                aggregation.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static DocBuilder docBuilder() {
        return new DocBuilder();
    }

    public static class DocBuilder implements ToXContent {

        private BytesReference doc;

        public DocBuilder setDoc(BytesReference doc) {
            this.doc = doc;
            return this;
        }

        public DocBuilder setDoc(String field, Object value) {
            Map<String, Object> values = new HashMap<>(2);
            values.put(field, value);
            setDoc(values);
            return this;
        }

        public DocBuilder setDoc(String doc) {
            this.doc = new BytesArray(doc);
            return this;
        }

        public DocBuilder setDoc(XContentBuilder doc) {
            this.doc = doc.bytes();
            return this;
        }

        public DocBuilder setDoc(Map doc) {
            return setDoc(doc, Requests.CONTENT_TYPE);
        }

        public DocBuilder setDoc(Map doc, XContentType contentType) {
            try {
                return setDoc(XContentFactory.contentBuilder(contentType).map(doc));
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + doc + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            XContentType contentType = XContentFactory.xContentType(doc);
            if (contentType == builder.contentType()) {
                builder.rawField("doc", doc);
            } else {
                try (XContentParser parser = XContentFactory.xContent(contentType).createParser(doc)) {
                    parser.nextToken();
                    builder.field("doc");
                    builder.copyCurrentStructure(parser);
                }
            }
            return builder;
        }
    }

}
