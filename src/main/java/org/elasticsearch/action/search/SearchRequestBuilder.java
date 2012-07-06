/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Map;

/**
 * A search action request builder.
 */
public class SearchRequestBuilder extends BaseRequestBuilder<SearchRequest, SearchResponse> {

    private SearchSourceBuilder sourceBuilder;

    public SearchRequestBuilder(Client client) {
        super(client, new SearchRequest());
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
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchRequestBuilder setTimeout(String timeout) {
        sourceBuilder().timeout(timeout);
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
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public SearchRequestBuilder setPreference(String preference) {
        request.preference(preference);
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
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public SearchRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchRequestBuilder setQuery(String query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchRequestBuilder setQuery(BytesReference queryBinary) {
        sourceBuilder().query(queryBinary);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchRequestBuilder setQuery(byte[] queryBinary) {
        sourceBuilder().query(queryBinary);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchRequestBuilder setQuery(byte[] queryBinary, int queryBinaryOffset, int queryBinaryLength) {
        sourceBuilder().query(queryBinary, queryBinaryOffset, queryBinaryLength);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchRequestBuilder setQuery(XContentBuilder query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchRequestBuilder setQuery(Map query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(FilterBuilder filter) {
        sourceBuilder().filter(filter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(String filter) {
        sourceBuilder().filter(filter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(BytesReference filter) {
        sourceBuilder().filter(filter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(byte[] filter) {
        sourceBuilder().filter(filter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(byte[] filter, int filterOffset, int filterLength) {
        sourceBuilder().filter(filter, filterOffset, filterLength);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(XContentBuilder filter) {
        sourceBuilder().filter(filter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchRequestBuilder setFilter(Map filter) {
        sourceBuilder().filter(filter);
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
     * The stats groups this request will be aggregated under.
     */
    public SearchRequestBuilder setStats(String... statsGroups) {
        sourceBuilder().stats(statsGroups);
        return this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be returned per field.
     */
    public SearchRequestBuilder setNoFields() {
        sourceBuilder().noFields();
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

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     */
    public SearchRequestBuilder addScriptField(String name, String script) {
        sourceBuilder().scriptField(name, script);
        return this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     * @param params Parameters that the script can use.
     */
    public SearchRequestBuilder addScriptField(String name, String script, Map<String, Object> params) {
        sourceBuilder().scriptField(name, script, params);
        return this;
    }

    /**
     * Adds a partial field based on _source, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param name    The name of the field
     * @param include An optional include (optionally wildcarded) pattern from _source
     * @param exclude An optional exclude (optionally wildcarded) pattern from _source
     */
    public SearchRequestBuilder addPartialField(String name, @Nullable String include, @Nullable String exclude) {
        sourceBuilder().partialField(name, include, exclude);
        return this;
    }

    /**
     * Adds a partial field based on _source, with an "includes" and/or "excludes set which can include simple wildcard
     * elements.
     *
     * @param name     The name of the field
     * @param includes An optional list of includes (optionally wildcarded) patterns from _source
     * @param excludes An optional list of excludes (optionally wildcarded) patterns from _source
     */
    public SearchRequestBuilder addPartialField(String name, @Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().partialField(name, includes, excludes);
        return this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param lang   The language of the script
     * @param script The script to use
     * @param params Parameters that the script can use (can be <tt>null</tt>).
     */
    public SearchRequestBuilder addScriptField(String name, String lang, String script, Map<String, Object> params) {
        sourceBuilder().scriptField(name, lang, script, params);
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
    public SearchRequestBuilder addSort(SortBuilder sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to
     * <tt>false</tt>.
     */
    public SearchRequestBuilder setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
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
     * Adds a facet to the search operation.
     */
    public SearchRequestBuilder addFacet(AbstractFacetBuilder facet) {
        sourceBuilder().facet(facet);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public SearchRequestBuilder setFacets(BytesReference facets) {
        sourceBuilder().facets(facets);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public SearchRequestBuilder setFacets(byte[] facets) {
        sourceBuilder().facets(facets);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public SearchRequestBuilder setFacets(byte[] facets, int facetsOffset, int facetsLength) {
        sourceBuilder().facets(facets, facetsOffset, facetsLength);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public SearchRequestBuilder setFacets(XContentBuilder facets) {
        sourceBuilder().facets(facets);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public SearchRequestBuilder setFacets(Map facets) {
        sourceBuilder().facets(facets);
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
     * Adds a field to be highlighted with a provided fragment size (in characters),
     * a provided (maximum) number of fragments and an offset for the highlight.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public SearchRequestBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments,
                                                    int fragmentOffset) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments, fragmentOffset);
        return this;
    }

    /**
     * Adds a highlighted field.
     */
    public SearchRequestBuilder addHighlightedField(HighlightBuilder.Field field) {
        highlightBuilder().field(field);
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
        highlightBuilder().postTags(postTags);
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
     * The encoder to set for highlighting
     */
    public SearchRequestBuilder setHighlighterEncoder(String encoder) {
        highlightBuilder().encoder(encoder);
        return this;
    }

    public SearchRequestBuilder setHighlighterRequireFieldMatch(boolean requireFieldMatch) {
        highlightBuilder().requireFieldMatch(requireFieldMatch);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(String)}.
     */
    public SearchRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public SearchRequestBuilder setExtraSource(String source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(BytesReference)}.
     */
    public SearchRequestBuilder setSource(BytesReference source) {
        request.source(source, false);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(BytesReference)}.
     */
    public SearchRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request.source(source, unsafe);
        return this;
    }


    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public SearchRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public SearchRequestBuilder setExtraSource(BytesReference source) {
        request.extraSource(source, false);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public SearchRequestBuilder setExtraSource(BytesReference source, boolean unsafe) {
        request.extraSource(source, unsafe);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public SearchRequestBuilder setExtraSource(byte[] source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public SearchRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public SearchRequestBuilder setExtraSource(byte[] source, int offset, int length) {
        request.extraSource(source, offset, length);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public SearchRequestBuilder setSource(XContentBuilder builder) {
        request.source(builder);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public SearchRequestBuilder setExtraSource(XContentBuilder builder) {
        request.extraSource(builder);
        return this;
    }

    /**
     * Sets the source of the request as a map. Note, setting anything other than the
     * search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(java.util.Map)}.
     */
    public SearchRequestBuilder setSource(Map source) {
        request.source(source);
        return this;
    }

    public SearchRequestBuilder setExtraSource(Map source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source builder to be used with this request. Note, any operations done
     * on this require builder before are discarded as this internal builder replaces
     * what has been built up until this point.
     */
    public SearchRequestBuilder internalBuilder(SearchSourceBuilder sourceBuilder) {
        this.sourceBuilder = sourceBuilder;
        return this;
    }

    /**
     * Returns the internal search source builder used to construct the request.
     */
    public SearchSourceBuilder internalBuilder() {
        return sourceBuilder();
    }

    @Override
    public String toString() {
        return internalBuilder().toString();
    }

    @Override
    public SearchRequest request() {
        if (sourceBuilder != null) {
            request.source(sourceBuilder());
        }
        return request;
    }

    @Override
    protected void doExecute(ActionListener<SearchResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder());
        }
        client.search(request, listener);
    }

    private SearchSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new SearchSourceBuilder();
        }
        return sourceBuilder;
    }

    private HighlightBuilder highlightBuilder() {
        return sourceBuilder().highlighter();
    }
}
