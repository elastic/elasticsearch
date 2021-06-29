/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A search action request builder.
 */
public class SearchRequestBuilder extends ActionRequestBuilder<SearchRequest, SearchResponse> {

    public SearchRequestBuilder(ElasticsearchClient client, SearchAction action) {
        super(client, action, new SearchRequest());
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequestBuilder setIndices(String... indices) {
        request.indices(indices);
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
     * one of "dfs_query_then_fetch" or "query_then_fetch".
     */
    public SearchRequestBuilder setSearchType(String searchType) {
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
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional document count, upon collecting which the search
     * query will early terminate
     */
    public SearchRequestBuilder setTerminateAfter(int terminateAfter) {
        sourceBuilder().terminateAfter(terminateAfter);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SearchRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SearchRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public SearchRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public SearchRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public SearchRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Sets a filter that will be executed after the query has been executed and only has affect on the search hits
     * (not aggregations). This filter is always executed as last filtering mechanism.
     */
    public SearchRequestBuilder setPostFilter(QueryBuilder postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public SearchRequestBuilder setMinScore(float minScore) {
        sourceBuilder().minScore(minScore);
        return this;
    }

    /**
     * From index to start the search from. Defaults to {@code 0}.
     */
    public SearchRequestBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }

    /**
     * The number of search hits to return. Defaults to {@code 10}.
     */
    public SearchRequestBuilder setSize(int size) {
        sourceBuilder().size(size);
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
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
     * version.
     */
    public SearchRequestBuilder setVersion(boolean version) {
        sourceBuilder().version(version);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with the
     * sequence number and primary term of the last modification of the document.
     */
    public SearchRequestBuilder seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        sourceBuilder().seqNoAndPrimaryTerm(seqNoAndPrimaryTerm);
        return this;
    }

    /**
     * Sets the boost a specific index will receive when the query is executed against it.
     *
     * @param index      The index to apply the boost against
     * @param indexBoost The boost to apply to the index
     */
    public SearchRequestBuilder addIndexBoost(String index, float indexBoost) {
        sourceBuilder().indexBoost(index, indexBoost);
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public SearchRequestBuilder setStats(String... statsGroups) {
        sourceBuilder().stats(Arrays.asList(statsGroups));
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public SearchRequestBuilder setStats(List<String> statsGroups) {
        sourceBuilder().stats(statsGroups);
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source for every hit
     */
    public SearchRequestBuilder setFetchSource(boolean fetch) {
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
    public SearchRequestBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
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
    public SearchRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().fetchSource(includes, excludes);
        return this;
    }

    /**
     * Adds a docvalue based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the docvalue
     */
    public SearchRequestBuilder addDocValueField(String name, String format) {
        sourceBuilder().docValueField(name, format);
        return this;
    }

    /**
     * Adds a docvalue based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the docvalue
     */
    public SearchRequestBuilder addDocValueField(String name) {
        return addDocValueField(name, null);
    }

    /**
     * Adds a field to load and return. The field must be present in the document _source.
     *
     * @param name The field to load
     */
    public SearchRequestBuilder addFetchField(String name) {
        sourceBuilder().fetchField(new FieldAndFormat(name, null, null));
        return this;
    }

    /**
     * Adds a field to load and return. The field must be present in the document _source.
     *
     * @param fetchField a {@link FieldAndFormat} specifying the field pattern, optional format (for example a date format) and
     * whether this field pattern sould also include unmapped fields
     */
    public SearchRequestBuilder addFetchField(FieldAndFormat fetchField) {
        sourceBuilder().fetchField(fetchField);
        return this;
    }

    /**
     * Adds a stored field to load and return (note, it must be stored) as part of the search request.
     */
    public SearchRequestBuilder addStoredField(String field) {
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
    public SearchRequestBuilder addScriptField(String name, Script script) {
        sourceBuilder().scriptField(name, script);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public SearchRequestBuilder addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.elasticsearch.search.sort.SortBuilders
     */
    public SearchRequestBuilder addSort(SortBuilder<?> sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Set the sort values that indicates which docs this request should "search after".
     *
     */
    public SearchRequestBuilder searchAfter(Object[] values) {
        sourceBuilder().searchAfter(values);
        return this;
    }

    public SearchRequestBuilder slice(SliceBuilder builder) {
        sourceBuilder().slice(builder);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to {@code false}.
     */
    public SearchRequestBuilder setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
        return this;
    }

    /**
     * Indicates if the total hit count for the query should be tracked. Requests will count total hit count accurately
     * up to 10,000 by default, see {@link #setTrackTotalHitsUpTo(int)} to change this value or set to true/false to always/never
     * count accurately.
     */
    public SearchRequestBuilder setTrackTotalHits(boolean trackTotalHits) {
        sourceBuilder().trackTotalHits(trackTotalHits);
        return this;
    }

    /**
     * Indicates the total hit count that should be tracked accurately or null if the value is unset. Defaults to 10,000.
     */
    public SearchRequestBuilder setTrackTotalHitsUpTo(int trackTotalHitsUpTo) {
        sourceBuilder().trackTotalHitsUpTo(trackTotalHitsUpTo);
        return this;
    }

    /**
     * Adds stored fields to load and return (note, it must be stored) as part of the search request.
     * To disable the stored fields entirely (source and metadata fields) use {@code storedField("_none_")}.
     */
    public SearchRequestBuilder storedFields(String... fields) {
        sourceBuilder().storedFields(Arrays.asList(fields));
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public SearchRequestBuilder addAggregation(AggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public SearchRequestBuilder addAggregation(PipelineAggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    public SearchRequestBuilder highlighter(HighlightBuilder highlightBuilder) {
        sourceBuilder().highlighter(highlightBuilder);
        return this;
    }

    /**
     * Delegates to {@link SearchSourceBuilder#suggest(SuggestBuilder)}
     */
    public SearchRequestBuilder suggest(SuggestBuilder suggestBuilder) {
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
    public SearchRequestBuilder setRescorer(RescorerBuilder<?> rescorer) {
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
    public SearchRequestBuilder setRescorer(RescorerBuilder rescorer, int window) {
        sourceBuilder().clearRescorers();
        return addRescorer(rescorer.windowSize(window));
    }

    /**
     * Adds a new rescorer.
     *
     * @param rescorer rescorer configuration
     * @return this for chaining
     */
    public SearchRequestBuilder addRescorer(RescorerBuilder<?> rescorer) {
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
    public SearchRequestBuilder addRescorer(RescorerBuilder<?> rescorer, int window) {
        sourceBuilder().addRescorer(rescorer.windowSize(window));
        return this;
    }

    /**
     * Clears all rescorers from the builder.
     *
     * @return this for chaining
     */
    public SearchRequestBuilder clearRescorers() {
        sourceBuilder().clearRescorers();
        return this;
    }

    /**
     * Sets the source of the request as a SearchSourceBuilder.
     */
    public SearchRequestBuilder setSource(SearchSourceBuilder source) {
        request.source(source);
        return this;
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if request cache is enabled or not).
     */
    public SearchRequestBuilder setRequestCache(Boolean requestCache) {
        request.requestCache(requestCache);
        return this;
    }


    /**
     * Sets if this request should allow partial results.  (If method is not called,
     * will default to the cluster level setting).
     */
    public SearchRequestBuilder setAllowPartialSearchResults(boolean allowPartialSearchResults) {
        request.allowPartialSearchResults(allowPartialSearchResults);
        return this;
    }

    /**
     * Should the query be profiled. Defaults to <code>false</code>
     */
    public SearchRequestBuilder setProfile(boolean profile) {
        sourceBuilder().profile(profile);
        return this;
    }

    public SearchRequestBuilder setCollapse(CollapseBuilder collapse) {
        sourceBuilder().collapse(collapse);
        return this;
    }

    /**
     * If specified, Elasticsearch will execute this search request using reader contexts from that point in time.
     */
    public SearchRequestBuilder setPointInTime(PointInTimeBuilder pointInTimeBuilder) {
        sourceBuilder().pointInTimeBuilder(pointInTimeBuilder);
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

    /**
     * Sets the number of shard results that should be reduced at once on the coordinating node. This value should be used as a protection
     * mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public SearchRequestBuilder setBatchedReduceSize(int batchedReduceSize) {
        this.request.setBatchedReduceSize(batchedReduceSize);
        return this;
    }

    /**
     * Sets the number of shard requests that should be executed concurrently on a single node. This value should be used as a
     * protection mechanism to reduce the number of shard requests fired per high level search request. Searches that hit the entire
     * cluster can be throttled with this number to reduce the cluster load. The default is {@code 5}.
     */
    public SearchRequestBuilder setMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
        this.request.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        return this;
    }

    /**
     * Sets a threshold that enforces a pre-filter roundtrip to pre-filter search shards based on query rewriting if the number of shards
     * the search request expands to exceeds the threshold. This filter roundtrip can limit the number of shards significantly if for
     * instance a shard can not match any documents based on its rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint.
     *
     * When unspecified, the pre-filter phase is executed if any of these conditions is met:
     * <ul>
     * <li>The request targets more than 128 shards</li>
     * <li>The request targets one or more read-only index</li>
     * <li>The primary sort of the query targets an indexed field</li>
     * </ul>
     */
    public SearchRequestBuilder setPreFilterShardSize(int preFilterShardSize) {
        this.request.setPreFilterShardSize(preFilterShardSize);
        return this;
    }

    /**
     * Set runtime mappings to create runtime fields that exist only as part of this particular search.
     */
    public SearchRequestBuilder setRuntimeMappings(Map<String, Object> runtimeMappings) {
        sourceBuilder().runtimeMappings(runtimeMappings);
        return this;
    }
}
