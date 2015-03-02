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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.util.Map;

/**
 * A search action request builder.
 */
public class SearchRequestBuilder extends ActionRequestBuilder<SearchRequest, SearchResponse, SearchRequestBuilder, Client> {

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
    public SearchRequestBuilder setSearchType(String searchType) throws ElasticsearchIllegalArgumentException {
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
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public SearchRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     *
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
     * Sets a filter that will be executed after the query has been executed and only has affect on the search hits
     * (not aggregations). This filter is always executed as last filtering mechanism.
     */
    public SearchRequestBuilder setPostFilter(FilterBuilder postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not aggs for example).
     */
    public SearchRequestBuilder setPostFilter(String postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not aggs for example).
     */
    public SearchRequestBuilder setPostFilter(BytesReference postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not aggs for example).
     */
    public SearchRequestBuilder setPostFilter(byte[] postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not aggs for example).
     */
    public SearchRequestBuilder setPostFilter(byte[] postFilter, int postFilterOffset, int postFilterLength) {
        sourceBuilder().postFilter(postFilter, postFilterOffset, postFilterLength);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not aggs for example).
     */
    public SearchRequestBuilder setPostFilter(XContentBuilder postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not aggs for example).
     */
    public SearchRequestBuilder setPostFilter(Map postFilter) {
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
     * Adds a field to load and return (note, it must be stored) as part of the search request.
     * If none are specified, the source of the document will be return.
     */
    public SearchRequestBuilder addField(String field) {
        sourceBuilder().field(field);
        return this;
    }

    /**
     * Adds a field data based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the field data cache
     */
    public SearchRequestBuilder addFieldDataField(String name) {
        sourceBuilder().fieldDataField(name);
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
     * Adds an get to the search operation.
     */
    public SearchRequestBuilder addAggregation(AbstractAggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of addAggregation to use.
     */
    public SearchRequestBuilder setAggregations(BytesReference aggregations) {
        sourceBuilder().aggregations(aggregations);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of addAggregation to use.
     */
    public SearchRequestBuilder setAggregations(byte[] aggregations) {
        sourceBuilder().aggregations(aggregations);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of addAggregation to use.
     */
    public SearchRequestBuilder setAggregations(byte[] aggregations, int aggregationsOffset, int aggregationsLength) {
        sourceBuilder().aggregations(aggregations, aggregationsOffset, aggregationsLength);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of addAggregation to use.
     */
    public SearchRequestBuilder setAggregations(XContentBuilder aggregations) {
        sourceBuilder().aggregations(aggregations);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of addAggregation to use.
     */
    public SearchRequestBuilder setAggregations(Map aggregations) {
        sourceBuilder().aggregations(aggregations);
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

    public SearchRequestBuilder setHighlighterFragmentSize(Integer fragmentSize) {
        highlightBuilder().fragmentSize(fragmentSize);
        return this;
    }

    public SearchRequestBuilder setHighlighterNumOfFragments(Integer numOfFragments) {
        highlightBuilder().numOfFragments(numOfFragments);
        return this;
    }

    public SearchRequestBuilder setHighlighterFilter(Boolean highlightFilter) {
        highlightBuilder().highlightFilter(highlightFilter);
        return this;
    }

    /**
     * The encoder to set for highlighting
     */
    public SearchRequestBuilder setHighlighterEncoder(String encoder) {
        highlightBuilder().encoder(encoder);
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

    public SearchRequestBuilder setHighlighterRequireFieldMatch(boolean requireFieldMatch) {
        highlightBuilder().requireFieldMatch(requireFieldMatch);
        return this;
    }

    public SearchRequestBuilder setHighlighterBoundaryMaxScan(Integer boundaryMaxScan) {
        highlightBuilder().boundaryMaxScan(boundaryMaxScan);
        return this;
    }

    public SearchRequestBuilder setHighlighterBoundaryChars(char[] boundaryChars) {
        highlightBuilder().boundaryChars(boundaryChars);
        return this;
    }

    /**
     * The highlighter type to use.
     */
    public SearchRequestBuilder setHighlighterType(String type) {
        highlightBuilder().highlighterType(type);
        return this;
    }

    public SearchRequestBuilder setHighlighterFragmenter(String fragmenter) {
        highlightBuilder().fragmenter(fragmenter);
        return this;
    }

    /**
     * Sets a query to be used for highlighting all fields instead of the search query.
     */
    public SearchRequestBuilder setHighlighterQuery(QueryBuilder highlightQuery) {
        highlightBuilder().highlightQuery(highlightQuery);
        return this;
    }

    /**
     * Sets the size of the fragment to return from the beginning of the field if there are no matches to
     * highlight and the field doesn't also define noMatchSize.
     * @param noMatchSize integer to set or null to leave out of request.  default is null.
     * @return this builder for chaining
     */
    public SearchRequestBuilder setHighlighterNoMatchSize(Integer noMatchSize) {
        highlightBuilder().noMatchSize(noMatchSize);
        return this;
    }

    /**
     * Sets the maximum number of phrases the fvh will consider if the field doesn't also define phraseLimit.
     */
    public SearchRequestBuilder setHighlighterPhraseLimit(Integer phraseLimit) {
        highlightBuilder().phraseLimit(phraseLimit);
        return this;
    }

    public SearchRequestBuilder setHighlighterOptions(Map<String, Object> options) {
        highlightBuilder().options(options);
        return this;
    }

    /**
     * Forces to highlight fields based on the source even if fields are stored separately.
     */
    public SearchRequestBuilder setHighlighterForceSource(Boolean forceSource) {
        highlightBuilder().forceSource(forceSource);
        return this;
    }

    /**
     * Send the fields to be highlighted using a syntax that is specific about the order in which they should be highlighted.
     * @return this for chaining
     */
    public SearchRequestBuilder setHighlighterExplicitFieldOrder(boolean explicitFieldOrder) {
        highlightBuilder().useExplicitFieldOrder(explicitFieldOrder);
        return this;
    }

    public SearchRequestBuilder addInnerHit(String name, InnerHitsBuilder.InnerHit innerHit) {
        innerHitsBuilder().addInnerHit(name, innerHit);
        return this;
    }

    /**
     * Delegates to {@link org.elasticsearch.search.suggest.SuggestBuilder#setText(String)}.
     */
    public SearchRequestBuilder setSuggestText(String globalText) {
        suggestBuilder().setText(globalText);
        return this;
    }

    /**
     * Delegates to {@link org.elasticsearch.search.suggest.SuggestBuilder#addSuggestion(org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder)}.
     */
    public SearchRequestBuilder addSuggestion(SuggestBuilder.SuggestionBuilder<?> suggestion) {
        suggestBuilder().addSuggestion(suggestion);
        return this;
    }

    /**
     * Clears all rescorers on the builder and sets the first one.  To use multiple rescore windows use
     * {@link #addRescorer(org.elasticsearch.search.rescore.RescoreBuilder.Rescorer, int)}.
     * @param rescorer rescorer configuration
     * @return this for chaining
     */
    public SearchRequestBuilder setRescorer(RescoreBuilder.Rescorer rescorer) {
        sourceBuilder().clearRescorers();
        return addRescorer(rescorer);
    }

    /**
     * Clears all rescorers on the builder and sets the first one.  To use multiple rescore windows use
     * {@link #addRescorer(org.elasticsearch.search.rescore.RescoreBuilder.Rescorer, int)}.
     * @param rescorer rescorer configuration
     * @param window rescore window
     * @return this for chaining
     */
    public SearchRequestBuilder setRescorer(RescoreBuilder.Rescorer rescorer, int window) {
        sourceBuilder().clearRescorers();
        return addRescorer(rescorer, window);
    }

    /**
     * Adds a new rescorer.
     * @param rescorer rescorer configuration
     * @return this for chaining
     */
    public SearchRequestBuilder addRescorer(RescoreBuilder.Rescorer rescorer) {
        sourceBuilder().addRescorer(new RescoreBuilder().rescorer(rescorer));
        return this;
    }

    /**
     * Adds a new rescorer.
     * @param rescorer rescorer configuration
     * @param window rescore window
     * @return this for chaining
     */
    public SearchRequestBuilder addRescorer(RescoreBuilder.Rescorer rescorer, int window) {
        sourceBuilder().addRescorer(new RescoreBuilder().rescorer(rescorer).windowSize(window));
        return this;
    }

    /**
     * Clears all rescorers from the builder.
     * @return this for chaining
     */
    public SearchRequestBuilder clearRescorers() {
        sourceBuilder().clearRescorers();
        return this;
    }

    /**
     * Sets the rescore window for all rescorers that don't specify a window when added.
     * @param window rescore window
     * @return this for chaining
     */
    public SearchRequestBuilder setRescoreWindow(int window) {
        sourceBuilder().defaultRescoreWindowSize(window);
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
        request.source(source);
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
        request.extraSource(source);
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
     * template stuff
     */

    public SearchRequestBuilder setTemplateName(String templateName) {
        request.templateName(templateName);
        return this;
    }

    public SearchRequestBuilder setTemplateType(ScriptService.ScriptType templateType) {
        request.templateType(templateType);
        return this;
    }

    public SearchRequestBuilder setTemplateParams(Map<String,Object> templateParams) {
        request.templateParams(templateParams);
        return this;
    }

    public SearchRequestBuilder setTemplateSource(String source) {
        request.templateSource(source);
        return this;
    }

    public SearchRequestBuilder setTemplateSource(BytesReference source) {
        request.templateSource(source);
        return this;
    }

    /**
     * Sets if this request should use the query cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if query cache is enabled or not).
     */
    public SearchRequestBuilder setQueryCache(Boolean queryCache) {
        request.queryCache(queryCache);
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
        if (sourceBuilder != null) {
            return sourceBuilder.toString();
        }
        if (request.source() != null) {
            try {
                return XContentHelper.convertToJson(request.source().toBytesArray(), false, true);
            } catch(Exception e) {
                return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
        return new SearchSourceBuilder().toString();
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

    private InnerHitsBuilder innerHitsBuilder() {
        return sourceBuilder().innerHitsBuilder();
    }

    private SuggestBuilder suggestBuilder() {
        return sourceBuilder().suggest();
    }
}
