/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.action.search;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceFacetsBuilder;
import org.elasticsearch.search.builder.SearchSourceHighlightBuilder;
import org.elasticsearch.search.facets.histogram.HistogramFacet;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A search action request builder.
 *
 * @author kimchy (shay.banon)
 */
public class SearchRequestBuilder {

    private final InternalClient client;

    private final SearchRequest request;

    private SearchSourceBuilder sourceBuilder;

    private SearchSourceFacetsBuilder facetsBuilder;

    private SearchSourceHighlightBuilder highlightBuilder;

    public SearchRequestBuilder(InternalClient client) {
        this.client = client;
        this.request = new SearchRequest();
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public SearchRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The search type to execute, defaults to {@link org.elasticsearch.action.search.SearchType#DEFAULT}.
     */
    public SearchRequestBuilder setSearchType(SearchType searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequestBuilder setSearchType(String searchType) throws ElasticSearchIllegalArgumentException {
        request.searchType(searchType);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * A query hint to optionally later be used when routing the request.
     */
    public SearchRequestBuilder setQueryHint(String queryHint) {
        request.queryHint(queryHint);
        return this;
    }

    /**
     * Controls the the search operation threading model.
     */
    public SearchRequestBuilder setOperationThreading(SearchOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    /**
     * Sets the string representation of the operation threading model. Can be one of
     * "no_threads", "single_thread" and "thread_per_shard".
     */
    public SearchRequestBuilder setOperationThreading(String operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public SearchRequestBuilder setListenerThreaded(boolean listenerThreaded) {
        request.listenerThreaded(listenerThreaded);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.xcontent.QueryBuilders
     */
    public SearchRequestBuilder setQuery(XContentQueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public SearchRequestBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public SearchRequestBuilder setSize(int size) {
        sourceBuilder().size(size);
        return this;
    }

    /**
     * An optional query parser name to use.
     */
    public SearchRequestBuilder setQueryParserName(String queryParserName) {
        sourceBuilder().queryParserName(queryParserName);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public SearchRequestBuilder setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return this;
    }

    /**
     * Sets the boost a specific index will receive when the query is executeed against it.
     *
     * @param index      The index to apply the boost against
     * @param indexBoost The boost to apply to the index
     */
    public SearchRequestBuilder addIndexBoost(String index, float indexBoost) {
        sourceBuilder().indexBoost(index, indexBoost);
        return this;
    }

    /**
     * Adds a field to load and return (note, it must be stored) as part of the search request.
     * If none are specified, the source of the document will be return.
     */
    public SearchRequestBuilder addField(String field) {
        sourceBuilder().field(field);
        return this;
    }

    public SearchRequestBuilder addScriptField(String name, String script) {
        sourceBuilder().scriptField(name, script);
        return this;
    }

    public SearchRequestBuilder addScriptField(String name, String script, Map<String, Object> params) {
        sourceBuilder().scriptField(name, script, params);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public SearchRequestBuilder addSort(String field, SearchSourceBuilder.Order order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds the fields to load and return as part of the search request. If none are specified,
     * the source of the document will be returned.
     */
    public SearchRequestBuilder addFields(String... fields) {
        sourceBuilder().fields(fields);
        return this;
    }

    /**
     * Adds a query facet (which results in a count facet returned).
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     * @see org.elasticsearch.search.facets.query.QueryFacet
     */
    public SearchRequestBuilder addFacetQuery(String name, XContentQueryBuilder query) {
        facetsBuilder().queryFacet(name, query);
        return this;
    }

    public SearchRequestBuilder addFacetQuery(String name, XContentQueryBuilder query, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().queryFacet(name, query, filter);
        return this;
    }

    /**
     * Adds a query facet (which results in a count facet returned) with an option to
     * be global on the index or bounded by the search query.
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     * @see org.elasticsearch.search.facets.query.QueryFacet
     */
    public SearchRequestBuilder addFacetGlobalQuery(String name, XContentQueryBuilder query) {
        facetsBuilder().queryFacetGlobal(name, query);
        return this;
    }

    public SearchRequestBuilder addFacetGlobalQuery(String name, XContentQueryBuilder query, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().queryFacetGlobal(name, query, filter);
        return this;
    }

    /**
     * Adds a term facet for the provided field name.
     *
     * @param name      The name of the facet
     * @param fieldName The field name to run the facet against
     * @param size      The number of the terms
     * @see org.elasticsearch.search.facets.terms.TermsFacet
     */
    public SearchRequestBuilder addFacetTerms(String name, String fieldName, int size) {
        facetsBuilder().termsFacet(name, fieldName, size);
        return this;
    }

    public SearchRequestBuilder addFacetTerms(String name, String fieldName, int size, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().termsFacet(name, fieldName, size, filter);
        return this;
    }

    /**
     * Adds a <b>global</b> term facet for the provided field name.
     *
     * @param name      The name of the facet
     * @param fieldName The field name to run the facet against
     * @param size      The number of the terms
     * @see org.elasticsearch.search.facets.terms.TermsFacet
     */
    public SearchRequestBuilder addFacetGlobalTerms(String name, String fieldName, int size) {
        facetsBuilder().termsFacetGlobal(name, fieldName, size);
        return this;
    }

    public SearchRequestBuilder addFacetGlobalTerms(String name, String fieldName, int size, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().termsFacetGlobal(name, fieldName, size, filter);
        return this;
    }

    /**
     * Adds a numeric statistical facet for the provided field name.
     *
     * @param name      The name of the facet
     * @param fieldName The name of the <b>numeric</b> field
     * @see org.elasticsearch.search.facets.statistical.StatisticalFacet
     */
    public SearchRequestBuilder addFacetStatistical(String name, String fieldName) {
        facetsBuilder().statisticalFacet(name, fieldName);
        return this;
    }

    public SearchRequestBuilder addFacetStatistical(String name, String fieldName, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().statisticalFacet(name, fieldName, filter);
        return this;
    }

    /**
     * Adds a numeric statistical <b>global</b> facet for the provided field name.
     *
     * @param name      The name of the facet
     * @param fieldName The name of the <b>numeric</b> field
     * @see org.elasticsearch.search.facets.statistical.StatisticalFacet
     */
    public SearchRequestBuilder addFacetGlobalStatistical(String name, String fieldName) {
        facetsBuilder().statisticalFacetGlobal(name, fieldName);
        return this;
    }

    public SearchRequestBuilder addFacetGlobalStatistical(String name, String fieldName, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().statisticalFacetGlobal(name, fieldName, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String fieldName, long interval) {
        facetsBuilder().histogramFacet(name, fieldName, interval);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String keyFieldName, String valueFieldName, long interval) {
        facetsBuilder().histogramFacet(name, keyFieldName, valueFieldName, interval);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String fieldName, long interval, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacet(name, fieldName, interval, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String keyFieldName, String valueFieldName, long interval, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacet(name, keyFieldName, valueFieldName, interval, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        facetsBuilder().histogramFacet(name, fieldName, interval, comparatorType);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        facetsBuilder().histogramFacet(name, keyFieldName, valueFieldName, interval, comparatorType);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                  @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacet(name, fieldName, interval, comparatorType, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogram(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                  @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacet(name, keyFieldName, valueFieldName, interval, comparatorType, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String fieldName, long interval) {
        facetsBuilder().histogramFacetGlobal(name, fieldName, interval);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String keyFieldName, String valueFieldName, long interval) {
        facetsBuilder().histogramFacetGlobal(name, keyFieldName, valueFieldName, interval);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String fieldName, long interval, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacetGlobal(name, fieldName, interval, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String keyFieldName, String valueFieldName, long interval, @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacetGlobal(name, keyFieldName, valueFieldName, interval, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        facetsBuilder().histogramFacetGlobal(name, fieldName, interval, comparatorType);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        facetsBuilder().histogramFacetGlobal(name, keyFieldName, valueFieldName, interval, comparatorType);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                        @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacetGlobal(name, fieldName, interval, comparatorType, filter);
        return this;
    }

    public SearchRequestBuilder addFacetHistogramGlobal(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                        @Nullable XContentFilterBuilder filter) {
        facetsBuilder().histogramFacetGlobal(name, keyFieldName, valueFieldName, interval, comparatorType, filter);
        return this;
    }

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5.
     *
     * @param name The field to highlight
     */
    public SearchRequestBuilder addHighlightedField(String name) {
        highlightBuilder().field(name);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * default number of fragments of 5.
     *
     * @param name         The field to highlight
     * @param fragmentSize The size of a fragment in characters
     */
    public SearchRequestBuilder addHighlightedField(String name, int fragmentSize) {
        highlightBuilder().field(name, fragmentSize);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public SearchRequestBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments);
        return this;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param schemaName The tag scheme name
     */
    public SearchRequestBuilder setHighlighterTagsSchema(String schemaName) {
        highlightBuilder().tagsSchema(schemaName);
        return this;
    }

    /**
     * Explicitly set the pre tags that will be used for highlighting.
     */
    public SearchRequestBuilder setHighlighterPreTags(String... preTags) {
        highlightBuilder().preTags(preTags);
        return this;
    }

    /**
     * Explicitly set the post tags that will be used for highlighting.
     */
    public SearchRequestBuilder setHighlighterPostTags(String... postTags) {
        highlightBuilder().preTags(postTags);
        return this;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the
     * highlighted text. Can be <tt>score</tt>, which then it will be ordered
     * by score of the fragments.
     */
    public SearchRequestBuilder setHighlighterOrder(String order) {
        highlightBuilder().order(order);
        return this;
    }

    /**
     * Executes the operation asynchronously and returns a future.
     */
    public ListenableActionFuture<SearchResponse> execute() {
        PlainListenableActionFuture<SearchResponse> future = new PlainListenableActionFuture<SearchResponse>(request.listenerThreaded(), client.threadPool());
        execute(future);
        return future;
    }

    /**
     * Executes the operation asynchronously with the provided listener.
     */
    public void execute(ActionListener<SearchResponse> listener) {
        if (facetsBuilder != null) {
            sourceBuilder().facets(facetsBuilder);
        }
        if (highlightBuilder != null) {
            sourceBuilder().highlight(highlightBuilder);
        }
        request.source(sourceBuilder());
        client.search(request, listener);
    }


    private SearchSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
        }
        return sourceBuilder;
    }

    private SearchSourceFacetsBuilder facetsBuilder() {
        if (facetsBuilder == null) {
            facetsBuilder = new SearchSourceFacetsBuilder();
        }
        return facetsBuilder;
    }

    private SearchSourceHighlightBuilder highlightBuilder() {
        if (highlightBuilder == null) {
            highlightBuilder = new SearchSourceHighlightBuilder();
        }
        return highlightBuilder;
    }
}
