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

package org.elasticsearch.search.builder;

import com.carrotsearch.hppc.ObjectFloatHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.Suggesters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A search source builder allowing to easily build search source. Simple
 * construction using
 * {@link org.elasticsearch.search.builder.SearchSourceBuilder#searchSource()}.
 *
 * @see org.elasticsearch.action.search.SearchRequest#source(SearchSourceBuilder)
 */
public final class SearchSourceBuilder extends ToXContentToBytes implements Writeable {

    public static final ParseField FROM_FIELD = new ParseField("from");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField TIMEOUT_FIELD = new ParseField("timeout");
    public static final ParseField TERMINATE_AFTER_FIELD = new ParseField("terminate_after");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField POST_FILTER_FIELD = new ParseField("post_filter");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
    public static final ParseField VERSION_FIELD = new ParseField("version");
    public static final ParseField EXPLAIN_FIELD = new ParseField("explain");
    public static final ParseField _SOURCE_FIELD = new ParseField("_source");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField STORED_FIELDS_FIELD = new ParseField("stored_fields");
    public static final ParseField DOCVALUE_FIELDS_FIELD = new ParseField("docvalue_fields", "fielddata_fields");
    public static final ParseField SCRIPT_FIELDS_FIELD = new ParseField("script_fields");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField IGNORE_FAILURE_FIELD = new ParseField("ignore_failure");
    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField TRACK_SCORES_FIELD = new ParseField("track_scores");
    public static final ParseField INDICES_BOOST_FIELD = new ParseField("indices_boost");
    public static final ParseField AGGREGATIONS_FIELD = new ParseField("aggregations");
    public static final ParseField AGGS_FIELD = new ParseField("aggs");
    public static final ParseField HIGHLIGHT_FIELD = new ParseField("highlight");
    public static final ParseField SUGGEST_FIELD = new ParseField("suggest");
    public static final ParseField RESCORE_FIELD = new ParseField("rescore");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField EXT_FIELD = new ParseField("ext");
    public static final ParseField PROFILE_FIELD = new ParseField("profile");
    public static final ParseField SEARCH_AFTER = new ParseField("search_after");
    public static final ParseField SLICE = new ParseField("slice");

    public static SearchSourceBuilder fromXContent(QueryParseContext context, AggregatorParsers aggParsers,
            Suggesters suggesters) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.parseXContent(context, aggParsers, suggesters);
        return builder;
    }

    /**
     * A static factory method to construct a new search source.
     */
    public static SearchSourceBuilder searchSource() {
        return new SearchSourceBuilder();
    }

    /**
     * A static factory method to construct new search highlights.
     */
    public static HighlightBuilder highlight() {
        return new HighlightBuilder();
    }

    private QueryBuilder queryBuilder;

    private QueryBuilder postQueryBuilder;

    private int from = -1;

    private int size = -1;

    private Boolean explain;

    private Boolean version;

    private List<SortBuilder<?>> sorts;

    private boolean trackScores = false;

    private SearchAfterBuilder searchAfterBuilder;

    private SliceBuilder sliceBuilder;

    private Float minScore;

    private TimeValue timeout = null;
    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;

    private List<String> storedFieldNames;
    private List<String> docValueFields;
    private List<ScriptField> scriptFields;
    private FetchSourceContext fetchSourceContext;

    private AggregatorFactories.Builder aggregations;

    private HighlightBuilder highlightBuilder;

    private SuggestBuilder suggestBuilder;

    private List<RescoreBuilder<?>> rescoreBuilders;

    private ObjectFloatHashMap<String> indexBoost = null;

    private List<String> stats;

    private BytesReference ext = null;

    private boolean profile = false;


    /**
     * Constructs a new search source builder.
     */
    public SearchSourceBuilder() {
    }

    /**
     * Read from a stream.
     */
    public SearchSourceBuilder(StreamInput in) throws IOException {
        aggregations = in.readOptionalWriteable(AggregatorFactories.Builder::new);
        explain = in.readOptionalBoolean();
        fetchSourceContext = in.readOptionalStreamable(FetchSourceContext::new);
        docValueFields = (List<String>) in.readGenericValue();
        storedFieldNames = (List<String>) in.readGenericValue();
        from = in.readVInt();
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        boolean hasIndexBoost = in.readBoolean();
        if (hasIndexBoost) {
            int size = in.readVInt();
            indexBoost = new ObjectFloatHashMap<>(size);
            for (int i = 0; i < size; i++) {
                indexBoost.put(in.readString(), in.readFloat());
            }
        }
        minScore = in.readOptionalFloat();
        postQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            int size = in.readVInt();
            rescoreBuilders = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                rescoreBuilders.add(in.readNamedWriteable(RescoreBuilder.class));
            }
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(new ScriptField(in));
            }
        }
        size = in.readVInt();
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                sorts.add(in.readNamedWriteable(SortBuilder.class));
            }
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            stats = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                stats.add(in.readString());
            }
        }
        suggestBuilder = in.readOptionalWriteable(SuggestBuilder::new);
        terminateAfter = in.readVInt();
        timeout = in.readOptionalWriteable(TimeValue::new);
        trackScores = in.readBoolean();
        version = in.readOptionalBoolean();
        ext = in.readOptionalBytesReference();
        profile = in.readBoolean();
        searchAfterBuilder = in.readOptionalWriteable(SearchAfterBuilder::new);
        sliceBuilder = in.readOptionalWriteable(SliceBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(aggregations);
        out.writeOptionalBoolean(explain);
        out.writeOptionalStreamable(fetchSourceContext);
        out.writeGenericValue(docValueFields);
        out.writeGenericValue(storedFieldNames);
        out.writeVInt(from);
        out.writeOptionalWriteable(highlightBuilder);
        boolean hasIndexBoost = indexBoost != null;
        out.writeBoolean(hasIndexBoost);
        if (hasIndexBoost) {
            out.writeVInt(indexBoost.size());
            for (ObjectCursor<String> key : indexBoost.keys()) {
                out.writeString(key.value);
                out.writeFloat(indexBoost.get(key.value));
            }
        }
        out.writeOptionalFloat(minScore);
        out.writeOptionalNamedWriteable(postQueryBuilder);
        out.writeOptionalNamedWriteable(queryBuilder);
        boolean hasRescoreBuilders = rescoreBuilders != null;
        out.writeBoolean(hasRescoreBuilders);
        if (hasRescoreBuilders) {
            out.writeVInt(rescoreBuilders.size());
            for (RescoreBuilder<?> rescoreBuilder : rescoreBuilders) {
                out.writeNamedWriteable(rescoreBuilder);
            }
        }
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            for (ScriptField scriptField : scriptFields) {
                scriptField.writeTo(out);
            }
        }
        out.writeVInt(size);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeNamedWriteable(sort);
            }
        }
        boolean hasStats = stats != null;
        out.writeBoolean(hasStats);
        if (hasStats) {
            out.writeVInt(stats.size());
            for (String stat : stats) {
                out.writeString(stat);
            }
        }
        out.writeOptionalWriteable(suggestBuilder);
        out.writeVInt(terminateAfter);
        out.writeOptionalWriteable(timeout);
        out.writeBoolean(trackScores);
        out.writeOptionalBoolean(version);
        out.writeOptionalBytesReference(ext);
        out.writeBoolean(profile);
        out.writeOptionalWriteable(searchAfterBuilder);
        out.writeOptionalWriteable(sliceBuilder);
    }

    /**
     * Sets the search query for this request.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public SearchSourceBuilder query(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    /**
     * Gets the query for this request
     */
    public QueryBuilder query() {
        return queryBuilder;
    }

    /**
     * Sets a filter that will be executed after the query has been executed and
     * only has affect on the search hits (not aggregations). This filter is
     * always executed as last filtering mechanism.
     */
    public SearchSourceBuilder postFilter(QueryBuilder postFilter) {
        this.postQueryBuilder = postFilter;
        return this;
    }

    /**
     * Gets the post filter for this request
     */
    public QueryBuilder postFilter() {
        return postQueryBuilder;
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public SearchSourceBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * Gets the from index to start the search from.
     **/
    public int from() {
        return from;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public SearchSourceBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Gets the number of search hits to return.
     */
    public int size() {
        return size;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public SearchSourceBuilder minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    /**
     * Gets the minimum score below which docs will be filtered out.
     */
    public Float minScore() {
        return minScore;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with
     * an explanation of the hit (ranking).
     */
    public SearchSourceBuilder explain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Indicates whether each search hit will be returned with an explanation of
     * the hit (ranking)
     */
    public Boolean explain() {
        return explain;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with a
     * version associated with it.
     */
    public SearchSourceBuilder version(Boolean version) {
        this.version = version;
        return this;
    }

    /**
     * Indicates whether the document's version will be included in the search
     * hits.
     */
    public Boolean version() {
        return version;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchSourceBuilder timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Gets the timeout to control how long search is allowed to take.
     */
    public TimeValue timeout() {
        return timeout;
    }

    /**
     * An optional terminate_after to terminate the search after collecting
     * <code>terminateAfter</code> documents
     */
    public  SearchSourceBuilder terminateAfter(int terminateAfter) {
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        }
        this.terminateAfter = terminateAfter;
        return this;
    }

    /**
     * Gets the number of documents to terminate after collecting.
     */
    public int terminateAfter() {
        return terminateAfter;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param name
     *            The name of the field
     * @param order
     *            The sort ordering
     */
    public SearchSourceBuilder sort(String name, SortOrder order) {
        if (name.equals(ScoreSortBuilder.NAME)) {
            return sort(SortBuilders.scoreSort().order(order));
        }
        return sort(SortBuilders.fieldSort(name).order(order));
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name
     *            The name of the field to sort by
     */
    public SearchSourceBuilder sort(String name) {
        if (name.equals(ScoreSortBuilder.NAME)) {
            return sort(SortBuilders.scoreSort());
        }
        return sort(SortBuilders.fieldSort(name));
    }

    /**
     * Adds a sort builder.
     */
    public SearchSourceBuilder sort(SortBuilder<?> sort) {
            if (sorts == null) {
                sorts = new ArrayList<>();
            }
            sorts.add(sort);
            return this;
    }

    /**
     * Gets the bytes representing the sort builders for this request.
     */
    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well.
     * Defaults to <tt>false</tt>.
     */
    public SearchSourceBuilder trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Indicates whether scores will be tracked for this request.
     */
    public boolean trackScores() {
        return trackScores;
    }


    /**
     * The sort values that indicates which docs this request should "search after".
     * The sort values of the search_after must be equal to the number of sort fields in the query and they should be
     * of the same type (or parsable as such).
     * Defaults to <tt>null</tt>.
     */
    public Object[] searchAfter() {
        if (searchAfterBuilder == null) {
            return null;
        }
        return searchAfterBuilder.getSortValues();
    }

    /**
     * Set the sort values that indicates which docs this request should "search after".
     */
    public SearchSourceBuilder searchAfter(Object[] values) {
        this.searchAfterBuilder = new SearchAfterBuilder().setSortValues(values);
        return this;
    }

    /**
     * Sets a filter that will restrict the search hits, the top hits and the aggregations to a slice of the results
     * of the main query.
     */
    public SearchSourceBuilder slice(SliceBuilder builder) {
        this.sliceBuilder = builder;
        return this;
    }

    /**
     * Gets the slice used to filter the search hits, the top hits and the aggregations.
     */
    public SliceBuilder slice() {
        return sliceBuilder;
    }

    /**
     * Add an aggregation to perform as part of the search.
     */
    public SearchSourceBuilder aggregation(AggregationBuilder aggregation) {
            if (aggregations == null) {
            aggregations = AggregatorFactories.builder();
            }
        aggregations.addAggregator(aggregation);
            return this;
    }

    /**
     * Add an aggregation to perform as part of the search.
     */
    public SearchSourceBuilder aggregation(PipelineAggregationBuilder aggregation) {
            if (aggregations == null) {
            aggregations = AggregatorFactories.builder();
            }
        aggregations.addPipelineAggregator(aggregation);
            return this;
        }

    /**
     * Gets the bytes representing the aggregation builders for this request.
     */
    public AggregatorFactories.Builder aggregations() {
        return aggregations;
    }

    /**
     * Adds highlight to perform as part of the search.
     */
    public SearchSourceBuilder highlighter(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Gets the highlighter builder for this request.
     */
    public HighlightBuilder highlighter() {
        return highlightBuilder;
    }

    public SearchSourceBuilder suggest(SuggestBuilder suggestBuilder) {
        this.suggestBuilder = suggestBuilder;
        return this;
    }

    /**
     * Gets the suggester builder for this request.
     */
    public SuggestBuilder suggest() {
        return suggestBuilder;
    }

    public SearchSourceBuilder addRescorer(RescoreBuilder<?> rescoreBuilder) {
            if (rescoreBuilders == null) {
                rescoreBuilders = new ArrayList<>();
            }
            rescoreBuilders.add(rescoreBuilder);
            return this;
    }

    public SearchSourceBuilder clearRescorers() {
        rescoreBuilders = null;
        return this;
    }

    /**
     * Should the query be profiled. Defaults to <tt>false</tt>
     */
    public SearchSourceBuilder profile(boolean profile) {
        this.profile = profile;
        return this;
    }

    /**
     * Return whether to profile query execution, or {@code null} if
     * unspecified.
     */
    public boolean profile() {
        return profile;
    }

    /**
     * Gets the bytes representing the rescore builders for this request.
     */
    public List<RescoreBuilder<?>> rescores() {
        return rescoreBuilders;
    }

    /**
     * Indicates whether the response should contain the stored _source for
     * every hit
     */
    public SearchSourceBuilder fetchSource(boolean fetch) {
        if (this.fetchSourceContext == null) {
            this.fetchSourceContext = new FetchSourceContext(fetch);
        } else {
            this.fetchSourceContext.fetchSource(fetch);
        }
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include
     *            An optional include (optionally wildcarded) pattern to filter
     *            the returned _source
     * @param exclude
     *            An optional exclude (optionally wildcarded) pattern to filter
     *            the returned _source
     */
    public SearchSourceBuilder fetchSource(@Nullable String include, @Nullable String exclude) {
        return fetchSource(include == null ? Strings.EMPTY_ARRAY : new String[] { include }, exclude == null ? Strings.EMPTY_ARRAY
                : new String[] { exclude });
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes
     *            An optional list of include (optionally wildcarded) pattern to
     *            filter the returned _source
     * @param excludes
     *            An optional list of exclude (optionally wildcarded) pattern to
     *            filter the returned _source
     */
    public SearchSourceBuilder fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        fetchSourceContext = new FetchSourceContext(includes, excludes);
        return this;
    }

    /**
     * Indicate how the _source should be fetched.
     */
    public SearchSourceBuilder fetchSource(@Nullable FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    /**
     * Gets the {@link FetchSourceContext} which defines how the _source should
     * be fetched.
     */
    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    /**
     * Adds a stored field to load and return as part of the
     * search request. If none are specified, the source of the document will be
     * return.
     */
    public SearchSourceBuilder storedField(String name) {
        if (storedFieldNames == null) {
            storedFieldNames = new ArrayList<>();
        }
        storedFieldNames.add(name);
        return this;
    }

    /**
     * Sets the stored fields to load and return as part of the search request. If none
     * are specified, the source of the document will be returned.
     */
    public SearchSourceBuilder storedFields(List<String> fields) {
        this.storedFieldNames = fields;
        return this;
    }

    /**
     * Sets no stored fields to be loaded, resulting in only id and type to be returned
     * per field.
     */
    public SearchSourceBuilder noStoredFields() {
        this.storedFieldNames = Collections.emptyList();
        return this;
    }

    /**
     * Gets the stored fields to load and return as part of the search request.
     */
    public List<String> storedFields() {
        return storedFieldNames;
    }


    /**
     * Adds a field to load from the docvalue and return as part of the
     * search request.
     *
     * @deprecated Use {@link SearchSourceBuilder#docValueField(String)} instead.
     */
    @Deprecated
    public SearchSourceBuilder fieldDataField(String name) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(name);
        return this;
    }

    /**
     * Gets the docvalue fields.
     *
     * @deprecated Use {@link SearchSourceBuilder#docValueFields()} instead.
     */
    @Deprecated
    public List<String> fieldDataFields() {
        return docValueFields;
    }


    /**
     * Gets the docvalue fields.
     */
    public List<String> docValueFields() {
        return docValueFields;
    }

    /**
     * Adds a field to load from the docvalue and return as part of the
     * search request.
     */
    public SearchSourceBuilder docValueField(String name) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(name);
        return this;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name
     *            The name of the field
     * @param script
     *            The script
     */
    public SearchSourceBuilder scriptField(String name, Script script) {
        scriptField(name, script, false);
        return this;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name
     *            The name of the field
     * @param script
     *            The script
     */
    public SearchSourceBuilder scriptField(String name, Script script, boolean ignoreFailure) {
        if (scriptFields == null) {
            scriptFields = new ArrayList<>();
        }
        scriptFields.add(new ScriptField(name, script, ignoreFailure));
        return this;
    }

    /**
     * Gets the script fields.
     */
    public List<ScriptField> scriptFields() {
        return scriptFields;
    }

    /**
     * Sets the boost a specific index will receive when the query is executed
     * against it.
     *
     * @param index
     *            The index to apply the boost against
     * @param indexBoost
     *            The boost to apply to the index
     */
    public SearchSourceBuilder indexBoost(String index, float indexBoost) {
        if (this.indexBoost == null) {
            this.indexBoost = new ObjectFloatHashMap<>();
        }
        this.indexBoost.put(index, indexBoost);
        return this;
    }

    /**
     * Gets the boost a specific indices will receive when the query is
     * executed against them.
     */
    public ObjectFloatHashMap<String> indexBoost() {
        return indexBoost;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public SearchSourceBuilder stats(List<String> statsGroups) {
        this.stats = statsGroups;
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public List<String> stats() {
        return stats;
    }

    public SearchSourceBuilder ext(XContentBuilder ext) {
        this.ext = ext.bytes();
        return this;
    }

    public BytesReference ext() {
        return ext;
    }

    /**
     * @return true if the source only has suggest
     */
    public boolean isSuggestOnly() {
        return suggestBuilder != null
            && queryBuilder == null && aggregations == null;
    }

    /**
     * Rewrites this search source builder into its primitive form. e.g. by
     * rewriting the QueryBuilder. If the builder did not change the identity
     * reference must be returned otherwise the builder will be rewritten
     * infinitely.
     */
    public SearchSourceBuilder rewrite(QueryShardContext context) throws IOException {
        assert (this.equals(shallowCopy(queryBuilder, postQueryBuilder)));
        QueryBuilder queryBuilder = null;
        if (this.queryBuilder != null) {
            queryBuilder = this.queryBuilder.rewrite(context);
        }
        QueryBuilder postQueryBuilder = null;
        if (this.postQueryBuilder != null) {
            postQueryBuilder = this.postQueryBuilder.rewrite(context);
        }
        boolean rewritten = queryBuilder != this.queryBuilder || postQueryBuilder != this.postQueryBuilder;
        if (rewritten) {
            return shallowCopy(queryBuilder, postQueryBuilder);
        }
        return this;
    }

    private SearchSourceBuilder shallowCopy(QueryBuilder queryBuilder, QueryBuilder postQueryBuilder) {
            SearchSourceBuilder rewrittenBuilder = new SearchSourceBuilder();
            rewrittenBuilder.aggregations = aggregations;
            rewrittenBuilder.explain = explain;
            rewrittenBuilder.ext = ext;
            rewrittenBuilder.fetchSourceContext = fetchSourceContext;
            rewrittenBuilder.docValueFields = docValueFields;
            rewrittenBuilder.storedFieldNames = storedFieldNames;
            rewrittenBuilder.from = from;
            rewrittenBuilder.highlightBuilder = highlightBuilder;
            rewrittenBuilder.indexBoost = indexBoost;
            rewrittenBuilder.minScore = minScore;
            rewrittenBuilder.postQueryBuilder = postQueryBuilder;
            rewrittenBuilder.profile = profile;
            rewrittenBuilder.queryBuilder = queryBuilder;
            rewrittenBuilder.rescoreBuilders = rescoreBuilders;
            rewrittenBuilder.scriptFields = scriptFields;
            rewrittenBuilder.searchAfterBuilder = searchAfterBuilder;
            rewrittenBuilder.sliceBuilder = sliceBuilder;
            rewrittenBuilder.size = size;
            rewrittenBuilder.sorts = sorts;
            rewrittenBuilder.stats = stats;
            rewrittenBuilder.suggestBuilder = suggestBuilder;
            rewrittenBuilder.terminateAfter = terminateAfter;
            rewrittenBuilder.timeout = timeout;
            rewrittenBuilder.trackScores = trackScores;
            rewrittenBuilder.version = version;
            return rewrittenBuilder;
        }

    /**
     * Parse some xContent into this SearchSourceBuilder, overwriting any values specified in the xContent. Use this if you need to set up
     * different defaults than a regular SearchSourceBuilder would have and use
     * {@link #fromXContent(QueryParseContext, AggregatorParsers, Suggesters)} if you have normal defaults.
     */
    public void parseXContent(QueryParseContext context, AggregatorParsers aggParsers, Suggesters suggesters)
        throws IOException {

        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]",
                    parser.getTokenLocation());
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (context.getParseFieldMatcher().match(currentFieldName, FROM_FIELD)) {
                    from = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, SIZE_FIELD)) {
                    size = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, TIMEOUT_FIELD)) {
                    timeout = TimeValue.parseTimeValue(parser.text(), null, TIMEOUT_FIELD.getPreferredName());
                } else if (context.getParseFieldMatcher().match(currentFieldName, TERMINATE_AFTER_FIELD)) {
                    terminateAfter = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, MIN_SCORE_FIELD)) {
                    minScore = parser.floatValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, VERSION_FIELD)) {
                    version = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, EXPLAIN_FIELD)) {
                    explain = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, TRACK_SCORES_FIELD)) {
                    trackScores = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    fetchSourceContext = FetchSourceContext.parse(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, STORED_FIELDS_FIELD)) {
                    storedField(parser.text());
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    sort(parser.text());
                } else if (context.getParseFieldMatcher().match(currentFieldName, PROFILE_FIELD)) {
                    profile = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    throw new ParsingException(parser.getTokenLocation(), "Deprecated field [" +
                        SearchSourceBuilder.FIELDS_FIELD + "] used, expected [" +
                        SearchSourceBuilder.STORED_FIELDS_FIELD + "] instead");
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    queryBuilder = context.parseInnerQueryBuilder().orElse(null);
                } else if (context.getParseFieldMatcher().match(currentFieldName, POST_FILTER_FIELD)) {
                    postQueryBuilder = context.parseInnerQueryBuilder().orElse(null);
                } else if (context.getParseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    fetchSourceContext = FetchSourceContext.parse(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SCRIPT_FIELDS_FIELD)) {
                    scriptFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        scriptFields.add(new ScriptField(context));
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, INDICES_BOOST_FIELD)) {
                    indexBoost = new ObjectFloatHashMap<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            indexBoost.put(currentFieldName, parser.floatValue());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                    parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, AGGREGATIONS_FIELD)
                        || context.getParseFieldMatcher().match(currentFieldName, AGGS_FIELD)) {
                    aggregations = aggParsers.parseAggregators(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, HIGHLIGHT_FIELD)) {
                    highlightBuilder = HighlightBuilder.fromXContent(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SUGGEST_FIELD)) {
                    suggestBuilder = SuggestBuilder.fromXContent(context, suggesters);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    sorts = new ArrayList<>(SortBuilder.fromXContent(context));
                } else if (context.getParseFieldMatcher().match(currentFieldName, RESCORE_FIELD)) {
                    rescoreBuilders = new ArrayList<>();
                    rescoreBuilders.add(RescoreBuilder.parseFromXContent(context));
                } else if (context.getParseFieldMatcher().match(currentFieldName, EXT_FIELD)) {
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().copyCurrentStructure(parser);
                    ext = xContentBuilder.bytes();
                } else if (context.getParseFieldMatcher().match(currentFieldName, SLICE)) {
                    sliceBuilder = SliceBuilder.fromXContent(context);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.getParseFieldMatcher().match(currentFieldName, STORED_FIELDS_FIELD)) {
                    storedFieldNames = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            storedFieldNames.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, DOCVALUE_FIELDS_FIELD)) {
                    docValueFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            docValueFields.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    sorts = new ArrayList<>(SortBuilder.fromXContent(context));
                } else if (context.getParseFieldMatcher().match(currentFieldName, RESCORE_FIELD)) {
                    rescoreBuilders = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rescoreBuilders.add(RescoreBuilder.parseFromXContent(context));
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, STATS_FIELD)) {
                    stats = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            stats.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    fetchSourceContext = FetchSourceContext.parse(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SEARCH_AFTER)) {
                    searchAfterBuilder = SearchAfterBuilder.fromXContent(parser, context.getParseFieldMatcher());
                } else if (context.getParseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    throw new ParsingException(parser.getTokenLocation(), "The field [" +
                        SearchSourceBuilder.FIELDS_FIELD + "] is no longer supported, please use [" +
                        SearchSourceBuilder.STORED_FIELDS_FIELD + "] to retrieve stored fields or _source filtering " +
                        "if the field is not stored");
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (from != -1) {
            builder.field(FROM_FIELD.getPreferredName(), from);
        }
        if (size != -1) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }

        if (timeout != null && !timeout.equals(TimeValue.MINUS_ONE)) {
            builder.field(TIMEOUT_FIELD.getPreferredName(), timeout.getStringRep());
        }

        if (terminateAfter != SearchContext.DEFAULT_TERMINATE_AFTER) {
            builder.field(TERMINATE_AFTER_FIELD.getPreferredName(), terminateAfter);
        }

        if (queryBuilder != null) {
            builder.field(QUERY_FIELD.getPreferredName(), queryBuilder);
        }

        if (postQueryBuilder != null) {
            builder.field(POST_FILTER_FIELD.getPreferredName(), postQueryBuilder);
        }

        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }

        if (version != null) {
            builder.field(VERSION_FIELD.getPreferredName(), version);
        }

        if (explain != null) {
            builder.field(EXPLAIN_FIELD.getPreferredName(), explain);
        }

        if (profile) {
            builder.field("profile", true);
        }

        if (fetchSourceContext != null) {
            builder.field(_SOURCE_FIELD.getPreferredName(), fetchSourceContext);
        }

        if (storedFieldNames != null) {
            if (storedFieldNames.size() == 1) {
                builder.field(STORED_FIELDS_FIELD.getPreferredName(), storedFieldNames.get(0));
            } else {
                builder.startArray(STORED_FIELDS_FIELD.getPreferredName());
                for (String fieldName : storedFieldNames) {
                    builder.value(fieldName);
                }
                builder.endArray();
            }
        }

        if (docValueFields != null) {
            builder.startArray(DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (String fieldDataField : docValueFields) {
                builder.value(fieldDataField);
            }
            builder.endArray();
        }

        if (scriptFields != null) {
            builder.startObject(SCRIPT_FIELDS_FIELD.getPreferredName());
            for (ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (sorts != null) {
            builder.startArray(SORT_FIELD.getPreferredName());
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (trackScores) {
            builder.field(TRACK_SCORES_FIELD.getPreferredName(), true);
        }

        if (searchAfterBuilder != null) {
            builder.field(SEARCH_AFTER.getPreferredName(), searchAfterBuilder.getSortValues());
        }

        if (sliceBuilder != null) {
            builder.field(SLICE.getPreferredName(), sliceBuilder);
        }

        if (indexBoost != null) {
            builder.startObject(INDICES_BOOST_FIELD.getPreferredName());
            assert !indexBoost.containsKey(null);
            final Object[] keys = indexBoost.keys;
            final float[] values = indexBoost.values;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != null) {
                    builder.field((String) keys[i], values[i]);
                }
            }
            builder.endObject();
        }

        if (aggregations != null) {
            builder.field(AGGREGATIONS_FIELD.getPreferredName(), aggregations);
            }

        if (highlightBuilder != null) {
            builder.field(HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder);
        }

        if (suggestBuilder != null) {
            builder.field(SUGGEST_FIELD.getPreferredName(), suggestBuilder);
        }

        if (rescoreBuilders != null) {
            builder.startArray(RESCORE_FIELD.getPreferredName());
            for (RescoreBuilder<?> rescoreBuilder : rescoreBuilders) {
                rescoreBuilder.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (stats != null) {
            builder.field(STATS_FIELD.getPreferredName(), stats);
        }

        if (ext != null) {
            builder.field(EXT_FIELD.getPreferredName());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(ext)) {
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }
        }
    }

    public static class ScriptField implements Writeable, ToXContent {

        private final boolean ignoreFailure;
        private final String fieldName;
        private final Script script;

        public ScriptField(String fieldName, Script script, boolean ignoreFailure) {
            this.fieldName = fieldName;
            this.script = script;
            this.ignoreFailure = ignoreFailure;
        }

        /**
         * Read from a stream.
         */
        public ScriptField(StreamInput in) throws IOException {
            fieldName = in.readString();
            script = new Script(in);
            ignoreFailure = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            script.writeTo(out);
            out.writeBoolean(ignoreFailure);
        }

        public ScriptField(QueryParseContext context) throws IOException {
            boolean ignoreFailure = false;
            XContentParser parser = context.parser();
            String scriptFieldName = parser.currentName();
            Script script = null;

            XContentParser.Token token;
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (context.getParseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                            script = Script.parse(parser, context.getParseFieldMatcher());
                        } else if (context.getParseFieldMatcher().match(currentFieldName, IGNORE_FAILURE_FIELD)) {
                            ignoreFailure = parser.booleanValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                    + "].", parser.getTokenLocation());
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (context.getParseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                            script = Script.parse(parser, context.getParseFieldMatcher());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                    + "].", parser.getTokenLocation());
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                + "].", parser.getTokenLocation());
                    }
                }
                this.ignoreFailure = ignoreFailure;
                this.fieldName = scriptFieldName;
                this.script = script;
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] in ["
                        + parser.currentName() + "] but found [" + token + "]", parser.getTokenLocation());
            }
        }

        public String fieldName() {
            return fieldName;
        }

        public Script script() {
            return script;
        }

        public boolean ignoreFailure() {
            return ignoreFailure;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(fieldName);
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.field(IGNORE_FAILURE_FIELD.getPreferredName(), ignoreFailure);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, script, ignoreFailure);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ScriptField other = (ScriptField) obj;
            return Objects.equals(fieldName, other.fieldName)
                    && Objects.equals(script, other.script)
                    && Objects.equals(ignoreFailure, other.ignoreFailure);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregations, explain, fetchSourceContext, docValueFields, storedFieldNames, from,
                highlightBuilder, indexBoost, minScore, postQueryBuilder, queryBuilder, rescoreBuilders, scriptFields,
                size, sorts, searchAfterBuilder, sliceBuilder, stats, suggestBuilder, terminateAfter, timeout, trackScores, version, profile);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SearchSourceBuilder other = (SearchSourceBuilder) obj;
        return Objects.equals(aggregations, other.aggregations)
                && Objects.equals(explain, other.explain)
                && Objects.equals(fetchSourceContext, other.fetchSourceContext)
                && Objects.equals(docValueFields, other.docValueFields)
                && Objects.equals(storedFieldNames, other.storedFieldNames)
                && Objects.equals(from, other.from)
                && Objects.equals(highlightBuilder, other.highlightBuilder)
                && Objects.equals(indexBoost, other.indexBoost)
                && Objects.equals(minScore, other.minScore)
                && Objects.equals(postQueryBuilder, other.postQueryBuilder)
                && Objects.equals(queryBuilder, other.queryBuilder)
                && Objects.equals(rescoreBuilders, other.rescoreBuilders)
                && Objects.equals(scriptFields, other.scriptFields)
                && Objects.equals(size, other.size)
                && Objects.equals(sorts, other.sorts)
                && Objects.equals(searchAfterBuilder, other.searchAfterBuilder)
                && Objects.equals(sliceBuilder, other.sliceBuilder)
                && Objects.equals(stats, other.stats)
                && Objects.equals(suggestBuilder, other.suggestBuilder)
                && Objects.equals(terminateAfter, other.terminateAfter)
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(trackScores, other.trackScores)
                && Objects.equals(version, other.version)
                && Objects.equals(profile, other.profile);
    }

}
