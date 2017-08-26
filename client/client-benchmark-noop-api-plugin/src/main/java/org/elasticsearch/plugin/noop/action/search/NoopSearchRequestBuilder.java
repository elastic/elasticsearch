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
package org.elasticsearch.plugin.noop.action.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.util.Arrays;
import java.util.List;

public class NoopSearchRequestBuilder extends ActionRequestBuilder<SearchRequest, SearchResponse, NoopSearchRequestBuilder> {

    public NoopSearchRequestBuilder(ElasticsearchClient client, NoopSearchAction action) {
        super(client, action, new SearchRequest());
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public NoopSearchRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public NoopSearchRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The search type to execute, defaults to {@link org.elasticsearch.action.search.SearchType#DEFAULT}.
     */
    public NoopSearchRequestBuilder setSearchType(SearchType searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link org.elasticsearch.action.search.SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public NoopSearchRequestBuilder setSearchType(String searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public NoopSearchRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public NoopSearchRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public NoopSearchRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public NoopSearchRequestBuilder setTimeout(TimeValue timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional document count, upon collecting which the search
     * query will early terminate
     */
    public NoopSearchRequestBuilder setTerminateAfter(int terminateAfter) {
        sourceBuilder().terminateAfter(terminateAfter);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public NoopSearchRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public NoopSearchRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public NoopSearchRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public NoopSearchRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public NoopSearchRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Sets a filter that will be executed after the query has been executed and only has affect on the search hits
     * (not aggregations). This filter is always executed as last filtering mechanism.
     */
    public NoopSearchRequestBuilder setPostFilter(QueryBuilder postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public NoopSearchRequestBuilder setMinScore(float minScore) {
        sourceBuilder().minScore(minScore);
        return this;
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public NoopSearchRequestBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public NoopSearchRequestBuilder setSize(int size) {
        sourceBuilder().size(size);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public NoopSearchRequestBuilder setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
     * version.
     */
    public NoopSearchRequestBuilder setVersion(boolean version) {
        sourceBuilder().version(version);
        return this;
    }

    /**
     * Sets the boost a specific index will receive when the query is executed against it.
     *
     * @param index      The index to apply the boost against
     * @param indexBoost The boost to apply to the index
     */
    public NoopSearchRequestBuilder addIndexBoost(String index, float indexBoost) {
        sourceBuilder().indexBoost(index, indexBoost);
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public NoopSearchRequestBuilder setStats(String... statsGroups) {
        sourceBuilder().stats(Arrays.asList(statsGroups));
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public NoopSearchRequestBuilder setStats(List<String> statsGroups) {
        sourceBuilder().stats(statsGroups);
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source for every hit
     */
    public NoopSearchRequestBuilder setFetchSource(boolean fetch) {
        sourceBuilder().fetchSource(fetch);
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include An optional include (optionally wildcarded) pattern to filter the returned _source
     * @param exclude An optional exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public NoopSearchRequestBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
        sourceBuilder().fetchSource(include, exclude);
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
     * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public NoopSearchRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().fetchSource(includes, excludes);
        return this;
    }

    /**
     * Adds a docvalue based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the docvalue
     */
    public NoopSearchRequestBuilder addDocValueField(String name) {
        sourceBuilder().docValueField(name);
        return this;
    }

    /**
     * Adds a stored field to load and return (note, it must be stored) as part of the search request.
     * If none are specified, the source of the document will be return.
     */
    public NoopSearchRequestBuilder addStoredField(String field) {
        sourceBuilder().storedField(field);
        return this;
    }


    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     */
    public NoopSearchRequestBuilder addScriptField(String name, Script script) {
        sourceBuilder().scriptField(name, script);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public NoopSearchRequestBuilder addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.elasticsearch.search.sort.SortBuilders
     */
    public NoopSearchRequestBuilder addSort(SortBuilder sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Set the sort values that indicates which docs this request should "search after".
     */
    public NoopSearchRequestBuilder searchAfter(Object[] values) {
        sourceBuilder().searchAfter(values);
        return this;
    }

    public NoopSearchRequestBuilder slice(SliceBuilder builder) {
        sourceBuilder().slice(builder);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to
     * <tt>false</tt>.
     */
    public NoopSearchRequestBuilder setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
        return this;
    }


    /**
     * Sets the fields to load and return as part of the search request. If none
     * are specified, the source of the document will be returned.
     */
    public NoopSearchRequestBuilder storedFields(String... fields) {
        sourceBuilder().storedFields(Arrays.asList(fields));
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public NoopSearchRequestBuilder addAggregation(AggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public NoopSearchRequestBuilder addAggregation(PipelineAggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    public NoopSearchRequestBuilder highlighter(HighlightBuilder highlightBuilder) {
        sourceBuilder().highlighter(highlightBuilder);
        return this;
    }

    /**
     * Delegates to {@link org.elasticsearch.search.builder.SearchSourceBuilder#suggest(SuggestBuilder)}
     */
    public NoopSearchRequestBuilder suggest(SuggestBuilder suggestBuilder) {
        sourceBuilder().suggest(suggestBuilder);
        return this;
    }

    /**
     * Clears all rescorers on the builder and sets the first one.  To use multiple rescore windows use
     * {@link #addRescorer(org.elasticsearch.search.rescore.RescorerBuilder, int)}.
     *
     * @param rescorer rescorer configuration
     * @return this for chaining
     */
    public NoopSearchRequestBuilder setRescorer(RescorerBuilder<?> rescorer) {
        sourceBuilder().clearRescorers();
        return addRescorer(rescorer);
    }

    /**
     * Clears all rescorers on the builder and sets the first one.  To use multiple rescore windows use
     * {@link #addRescorer(org.elasticsearch.search.rescore.RescorerBuilder, int)}.
     *
     * @param rescorer rescorer configuration
     * @param window   rescore window
     * @return this for chaining
     */
    public NoopSearchRequestBuilder setRescorer(RescorerBuilder rescorer, int window) {
        sourceBuilder().clearRescorers();
        return addRescorer(rescorer.windowSize(window));
    }

    /**
     * Adds a new rescorer.
     *
     * @param rescorer rescorer configuration
     * @return this for chaining
     */
    public NoopSearchRequestBuilder addRescorer(RescorerBuilder<?> rescorer) {
        sourceBuilder().addRescorer(rescorer);
        return this;
    }

    /**
     * Adds a new rescorer.
     *
     * @param rescorer rescorer configuration
     * @param window   rescore window
     * @return this for chaining
     */
    public NoopSearchRequestBuilder addRescorer(RescorerBuilder<?> rescorer, int window) {
        sourceBuilder().addRescorer(rescorer.windowSize(window));
        return this;
    }

    /**
     * Clears all rescorers from the builder.
     *
     * @return this for chaining
     */
    public NoopSearchRequestBuilder clearRescorers() {
        sourceBuilder().clearRescorers();
        return this;
    }

    /**
     * Sets the source of the request as a SearchSourceBuilder.
     */
    public NoopSearchRequestBuilder setSource(SearchSourceBuilder source) {
        request.source(source);
        return this;
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if request cache is enabled or not).
     */
    public NoopSearchRequestBuilder setRequestCache(Boolean requestCache) {
        request.requestCache(requestCache);
        return this;
    }

    /**
     * Should the query be profiled. Defaults to <code>false</code>
     */
    public NoopSearchRequestBuilder setProfile(boolean profile) {
        sourceBuilder().profile(profile);
        return this;
    }

    @Override
    public String toString() {
        if (request.source() != null) {
            return request.source().toString();
        }
        return new SearchSourceBuilder().toString();
    }

    private SearchSourceBuilder sourceBuilder() {
        if (request.source() == null) {
            request.source(new SearchSourceBuilder());
        }
        return request.source();
    }
}
