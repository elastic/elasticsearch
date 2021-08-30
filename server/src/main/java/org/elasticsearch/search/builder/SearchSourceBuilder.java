/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

/**
 * A search source builder allowing to easily build search source. Simple
 * construction using
 * {@link org.elasticsearch.search.builder.SearchSourceBuilder#searchSource()}.
 *
 * @see org.elasticsearch.action.search.SearchRequest#source(SearchSourceBuilder)
 */
public final class SearchSourceBuilder implements Writeable, ToXContentObject, Rewriteable<SearchSourceBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SearchSourceBuilder.class);

    public static final ParseField FROM_FIELD = new ParseField("from");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField TIMEOUT_FIELD = new ParseField("timeout");
    public static final ParseField TERMINATE_AFTER_FIELD = new ParseField("terminate_after");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField POST_FILTER_FIELD = new ParseField("post_filter");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
    public static final ParseField VERSION_FIELD = new ParseField("version");
    public static final ParseField SEQ_NO_PRIMARY_TERM_FIELD = new ParseField("seq_no_primary_term");
    public static final ParseField EXPLAIN_FIELD = new ParseField("explain");
    public static final ParseField _SOURCE_FIELD = new ParseField("_source");
    public static final ParseField STORED_FIELDS_FIELD = new ParseField("stored_fields");
    public static final ParseField DOCVALUE_FIELDS_FIELD = new ParseField("docvalue_fields");
    public static final ParseField FETCH_FIELDS_FIELD = new ParseField("fields");
    public static final ParseField SCRIPT_FIELDS_FIELD = new ParseField("script_fields");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField IGNORE_FAILURE_FIELD = new ParseField("ignore_failure");
    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField TRACK_SCORES_FIELD = new ParseField("track_scores");
    public static final ParseField TRACK_TOTAL_HITS_FIELD = new ParseField("track_total_hits");
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
    public static final ParseField COLLAPSE = new ParseField("collapse");
    public static final ParseField SLICE = new ParseField("slice");
    public static final ParseField POINT_IN_TIME = new ParseField("pit");
    public static final ParseField RUNTIME_MAPPINGS_FIELD = new ParseField("runtime_mappings");

    public static SearchSourceBuilder fromXContent(XContentParser parser) throws IOException {
        return fromXContent(parser, true);
    }

    public static SearchSourceBuilder fromXContent(XContentParser parser, boolean checkTrailingTokens) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.parseXContent(parser, checkTrailingTokens);
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

    private Boolean seqNoAndPrimaryTerm;

    private List<SortBuilder<?>> sorts;

    private boolean trackScores = false;

    private Integer trackTotalHitsUpTo;

    private SearchAfterBuilder searchAfterBuilder;

    private SliceBuilder sliceBuilder;

    private Float minScore;

    private TimeValue timeout = null;
    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;

    private StoredFieldsContext storedFieldsContext;
    private List<FieldAndFormat> docValueFields;
    private List<ScriptField> scriptFields;
    private FetchSourceContext fetchSourceContext;
    private List<FieldAndFormat> fetchFields;

    private AggregatorFactories.Builder aggregations;

    private HighlightBuilder highlightBuilder;

    private SuggestBuilder suggestBuilder;

    @SuppressWarnings("rawtypes")
    private List<RescorerBuilder> rescoreBuilders;

    private List<IndexBoost> indexBoosts = new ArrayList<>();

    private List<String> stats;

    private List<SearchExtBuilder> extBuilders = Collections.emptyList();

    private boolean profile = false;

    private CollapseBuilder collapse = null;

    private PointInTimeBuilder pointInTimeBuilder = null;

    private Map<String, Object> runtimeMappings = emptyMap();

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
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.getVersion().before(Version.V_6_4_0)) {
            @SuppressWarnings("unchecked")
            List<String> dvFields = (List<String>) in.readGenericValue();
            if (dvFields == null) {
                docValueFields = null;
            } else {
                docValueFields = dvFields.stream()
                        .map(field -> new FieldAndFormat(field, null))
                        .collect(Collectors.toList());
            }
        } else {
            if (in.readBoolean()) {
                docValueFields = in.readList(FieldAndFormat::new);
            } else {
                docValueFields = null;
            }
        }
        storedFieldsContext = in.readOptionalWriteable(StoredFieldsContext::new);
        from = in.readVInt();
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        indexBoosts = in.readList(IndexBoost::new);
        minScore = in.readOptionalFloat();
        postQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            rescoreBuilders = in.readNamedWriteableList(RescorerBuilder.class);
        }
        if (in.readBoolean()) {
            scriptFields = in.readList(ScriptField::new);
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
            stats = in.readStringList();
        }
        suggestBuilder = in.readOptionalWriteable(SuggestBuilder::new);
        terminateAfter = in.readVInt();
        timeout = in.readOptionalTimeValue();
        trackScores = in.readBoolean();
        version = in.readOptionalBoolean();
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
            seqNoAndPrimaryTerm = in.readOptionalBoolean();
        } else {
            seqNoAndPrimaryTerm = null;
        }
        extBuilders = in.readNamedWriteableList(SearchExtBuilder.class);
        profile = in.readBoolean();
        searchAfterBuilder = in.readOptionalWriteable(SearchAfterBuilder::new);
        sliceBuilder = in.readOptionalWriteable(SliceBuilder::new);
        collapse = in.readOptionalWriteable(CollapseBuilder::new);
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            trackTotalHitsUpTo = in.readOptionalInt();
        } else {
            trackTotalHitsUpTo = in.readBoolean() ? TRACK_TOTAL_HITS_ACCURATE : TRACK_TOTAL_HITS_DISABLED;
        }
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            if (in.readBoolean()) {
                fetchFields = in.readList(FieldAndFormat::new);
            }
        }
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            pointInTimeBuilder = in.readOptionalWriteable(PointInTimeBuilder::new);
        }
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            runtimeMappings = in.readMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(aggregations);
        out.writeOptionalBoolean(explain);
        out.writeOptionalWriteable(fetchSourceContext);
        if (out.getVersion().before(Version.V_6_4_0)) {
            out.writeGenericValue(docValueFields == null
                    ? null
                    : docValueFields.stream().map(ff -> ff.field).collect(Collectors.toList()));
        } else {
            out.writeBoolean(docValueFields != null);
            if (docValueFields != null) {
                out.writeList(docValueFields);
            }
        }
        out.writeOptionalWriteable(storedFieldsContext);
        out.writeVInt(from);
        out.writeOptionalWriteable(highlightBuilder);
        out.writeList(indexBoosts);
        out.writeOptionalFloat(minScore);
        out.writeOptionalNamedWriteable(postQueryBuilder);
        out.writeOptionalNamedWriteable(queryBuilder);
        boolean hasRescoreBuilders = rescoreBuilders != null;
        out.writeBoolean(hasRescoreBuilders);
        if (hasRescoreBuilders) {
            out.writeNamedWriteableList(rescoreBuilders);
        }
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeList(scriptFields);
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
            out.writeStringCollection(stats);
        }
        out.writeOptionalWriteable(suggestBuilder);
        out.writeVInt(terminateAfter);
        out.writeOptionalTimeValue(timeout);
        out.writeBoolean(trackScores);
        out.writeOptionalBoolean(version);
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
            out.writeOptionalBoolean(seqNoAndPrimaryTerm);
        }
        out.writeNamedWriteableList(extBuilders);
        out.writeBoolean(profile);
        out.writeOptionalWriteable(searchAfterBuilder);
        out.writeOptionalWriteable(sliceBuilder);
        out.writeOptionalWriteable(collapse);
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeOptionalInt(trackTotalHitsUpTo);
        } else {
            out.writeBoolean(trackTotalHitsUpTo == null ? true : trackTotalHitsUpTo > SearchContext.TRACK_TOTAL_HITS_DISABLED);
        }
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeBoolean(fetchFields != null);
            if (fetchFields != null) {
                out.writeList(fetchFields);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeOptionalWriteable(pointInTimeBuilder);
        }
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeMap(runtimeMappings);
        } else {
            if (false == runtimeMappings.isEmpty()) {
                throw new IllegalArgumentException(
                    "Versions before 7.11.0 don't support [runtime_mappings] and search was sent to [" + out.getVersion() + "]"
                );
            }
        }
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
     * From index to start the search from. Defaults to {@code 0}.
     */
    public SearchSourceBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[from] parameter cannot be negative");
        }
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
     * The number of search hits to return. Defaults to {@code 10}.
     */
    public SearchSourceBuilder size(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("[size] parameter cannot be negative, found [" + size + "]");
        }
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
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with the
     * sequence number and primary term of the last modification of the document.
     */
    public SearchSourceBuilder seqNoAndPrimaryTerm(Boolean seqNoAndPrimaryTerm) {
        this.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
        return this;
    }

    /**
     * Indicates whether {@link org.elasticsearch.search.SearchHit}s should be returned with the
     * sequence number and primary term of the last modification of the document.
     */
    public Boolean seqNoAndPrimaryTerm() {
        return seqNoAndPrimaryTerm;
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
     * Sets the sort builders for this request.
     */
    public SearchSourceBuilder sort(List<SortBuilder<?>> sorts) {
        this.sorts = sorts;
        return this;
    }

    /**
     * Gets the sort builders for this request.
     */
    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well.
     * Defaults to {@code false}.
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
     * Indicates if the total hit count for the query should be tracked.
     */
    public SearchSourceBuilder trackTotalHits(boolean trackTotalHits) {
        this.trackTotalHitsUpTo = trackTotalHits ? TRACK_TOTAL_HITS_ACCURATE : TRACK_TOTAL_HITS_DISABLED;
        return this;
    }

    /**
     * Returns the total hit count that should be tracked or null if the value is unset.
     * Defaults to null.
     */
    @Nullable
    public Integer trackTotalHitsUpTo() {
        return trackTotalHitsUpTo;
    }

    public SearchSourceBuilder trackTotalHitsUpTo(int trackTotalHitsUpTo) {
        if (trackTotalHitsUpTo < TRACK_TOTAL_HITS_DISABLED) {
            throw new IllegalArgumentException("[track_total_hits] parameter must be positive or equals to -1, " +
                "got " + trackTotalHitsUpTo);
        }
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        return this;
    }

    /**
     * The sort values that indicates which docs this request should "search after".
     * The sort values of the search_after must be equal to the number of sort fields in the query and they should be
     * of the same type (or parsable as such).
     * Defaults to {@code null}.
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


    public CollapseBuilder collapse() {
        return collapse;
    }

    public SearchSourceBuilder collapse(CollapseBuilder collapse) {
        this.collapse = collapse;
        return this;
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

    public SearchSourceBuilder addRescorer(RescorerBuilder<?> rescoreBuilder) {
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
     * Should the query be profiled. Defaults to {@code false}
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
    @SuppressWarnings("rawtypes")
    public List<RescorerBuilder> rescores() {
        return rescoreBuilders;
    }

    /**
     * Indicates whether the response should contain the stored _source for
     * every hit
     */
    public SearchSourceBuilder fetchSource(boolean fetch) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext
            : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetch, fetchSourceContext.includes(), fetchSourceContext.excludes());
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
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext
            : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(), includes, excludes);
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
        return storedFields(Collections.singletonList(name));
    }

    /**
     * Sets the stored fields to load and return as part of the search request. If none
     * are specified, the source of the document will be returned.
     */
    public SearchSourceBuilder storedFields(List<String> fields) {
        if (storedFieldsContext == null) {
            storedFieldsContext = StoredFieldsContext.fromList(fields);
        } else {
            storedFieldsContext.addFieldNames(fields);
        }
        return this;
    }

    /**
     * Indicates how the stored fields should be fetched.
     */
    public SearchSourceBuilder storedFields(StoredFieldsContext context) {
        storedFieldsContext = context;
        return this;
    }

    /**
     * Gets the stored fields context.
     */
    public StoredFieldsContext storedFields() {
        return storedFieldsContext;
    }

    /**
     * Gets the docvalue fields.
     */
    public List<FieldAndFormat> docValueFields() {
        return docValueFields;
    }

    /**
     * Adds a field to load from the doc values and return as part of the
     * search request.
     */
    public SearchSourceBuilder docValueField(String name, @Nullable String format) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(new FieldAndFormat(name, format));
        return this;
    }

    /**
     * Adds a field to load from the doc values and return as part of the
     * search request.
     */
    public SearchSourceBuilder docValueField(String name) {
        return docValueField(name, null);
    }

    /**
     * Gets the fields to load and return as part of the search request.
     */
    public List<FieldAndFormat> fetchFields() {
        return fetchFields;
    }

    /**
     * Adds a field to load and return as part of the search request.
     */
    public SearchSourceBuilder fetchField(String name) {
        return fetchField(new FieldAndFormat(name, null, null));
    }

    /**
     * Adds a field to load and return as part of the search request.
     * @param fetchField defining the field name, optional format and optional inclusion of unmapped fields
     */
    public SearchSourceBuilder fetchField(FieldAndFormat fetchField) {
        if (fetchFields == null) {
            fetchFields = new ArrayList<>();
        }
        fetchFields.add(fetchField);
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
     * Sets the boost a specific index or alias will receive when the query is executed
     * against it.
     *
     * @param index
     *            The index or alias to apply the boost against
     * @param indexBoost
     *            The boost to apply to the index
     */
    public SearchSourceBuilder indexBoost(String index, float indexBoost) {
        Objects.requireNonNull(index, "index must not be null");
        this.indexBoosts.add(new IndexBoost(index, indexBoost));
        return this;
    }

    /**
     * Gets the boost a specific indices or aliases will receive when the query is
     * executed against them.
     */
    public List<IndexBoost> indexBoosts() {
        return indexBoosts;
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

    public SearchSourceBuilder ext(List<SearchExtBuilder> searchExtBuilders) {
        this.extBuilders = Objects.requireNonNull(searchExtBuilders, "searchExtBuilders must not be null");
        return this;
    }

    public List<SearchExtBuilder> ext() {
        return extBuilders;
    }

    /**
     * @return true if the source only has suggest
     */
    public boolean isSuggestOnly() {
        return suggestBuilder != null
            && queryBuilder == null && aggregations == null;
    }

    /**
     * Returns the point in time that is configured with this query
     */
    public PointInTimeBuilder pointInTimeBuilder() {
        return pointInTimeBuilder;
    }

    /**
     * Specify a point in time that this query should execute against.
     */
    public SearchSourceBuilder pointInTimeBuilder(PointInTimeBuilder builder) {
        this.pointInTimeBuilder = builder;
        return this;
    }

    /**
     * Mappings specified on this search request that override built in mappings.
     */
    public Map<String, Object> runtimeMappings() {
        return Collections.unmodifiableMap(runtimeMappings);
    }

    /**
     * Specify the mappings specified on this search request that override built in mappings.
     */
    public SearchSourceBuilder runtimeMappings(Map<String, Object> runtimeMappings) {
        this.runtimeMappings = runtimeMappings == null ? emptyMap() : runtimeMappings;
        return this;
    }

    /**
     * Rewrites this search source builder into its primitive form. e.g. by
     * rewriting the QueryBuilder. If the builder did not change the identity
     * reference must be returned otherwise the builder will be rewritten
     * infinitely.
     */
    @Override
    public SearchSourceBuilder rewrite(QueryRewriteContext context) throws IOException {
        assert (this.equals(shallowCopy(queryBuilder, postQueryBuilder, aggregations, sliceBuilder, sorts, rescoreBuilders,
            highlightBuilder)));
        QueryBuilder queryBuilder = null;
        if (this.queryBuilder != null) {
            queryBuilder = this.queryBuilder.rewrite(context);
        }
        QueryBuilder postQueryBuilder = null;
        if (this.postQueryBuilder != null) {
            postQueryBuilder = this.postQueryBuilder.rewrite(context);
        }
        AggregatorFactories.Builder aggregations = null;
        if (this.aggregations != null) {
            aggregations = this.aggregations.rewrite(context);
        }
        List<SortBuilder<?>> sorts = Rewriteable.rewrite(this.sorts, context);

        @SuppressWarnings({"rawtypes", "unchecked"})
        List<RescorerBuilder> rescoreBuilders = Rewriteable.rewrite(this.rescoreBuilders, context);
        HighlightBuilder highlightBuilder = this.highlightBuilder;
        if (highlightBuilder != null) {
            highlightBuilder = this.highlightBuilder.rewrite(context);
        }

        boolean rewritten = queryBuilder != this.queryBuilder || postQueryBuilder != this.postQueryBuilder
                || aggregations != this.aggregations || rescoreBuilders != this.rescoreBuilders || sorts != this.sorts ||
                this.highlightBuilder != highlightBuilder;
        if (rewritten) {
            return shallowCopy(queryBuilder, postQueryBuilder, aggregations, this.sliceBuilder, sorts, rescoreBuilders, highlightBuilder);
        }
        return this;
    }

    /**
     * Create a shallow copy of this builder with a new slice configuration.
     */
    public SearchSourceBuilder shallowCopy() {
        return shallowCopy(queryBuilder, postQueryBuilder, aggregations, sliceBuilder, sorts, rescoreBuilders, highlightBuilder);
    }

    /**
     * Create a shallow copy of this source replaced {@link #queryBuilder}, {@link #postQueryBuilder}, and {@link #sliceBuilder}. Used by
     * {@link #rewrite(QueryRewriteContext)}}.
     */
    private SearchSourceBuilder shallowCopy(QueryBuilder queryBuilder, QueryBuilder postQueryBuilder,
                                            AggregatorFactories.Builder aggregations, SliceBuilder slice, List<SortBuilder<?>> sorts,
                                            @SuppressWarnings("rawtypes") List<RescorerBuilder> rescoreBuilders,
                                            HighlightBuilder highlightBuilder) {
        SearchSourceBuilder rewrittenBuilder = new SearchSourceBuilder();
        rewrittenBuilder.aggregations = aggregations;
        rewrittenBuilder.explain = explain;
        rewrittenBuilder.extBuilders = extBuilders;
        rewrittenBuilder.fetchSourceContext = fetchSourceContext;
        rewrittenBuilder.fetchFields = fetchFields;
        rewrittenBuilder.docValueFields = docValueFields;
        rewrittenBuilder.storedFieldsContext = storedFieldsContext;
        rewrittenBuilder.from = from;
        rewrittenBuilder.highlightBuilder = highlightBuilder;
        rewrittenBuilder.indexBoosts = indexBoosts;
        rewrittenBuilder.minScore = minScore;
        rewrittenBuilder.postQueryBuilder = postQueryBuilder;
        rewrittenBuilder.profile = profile;
        rewrittenBuilder.queryBuilder = queryBuilder;
        rewrittenBuilder.rescoreBuilders = rescoreBuilders;
        rewrittenBuilder.scriptFields = scriptFields;
        rewrittenBuilder.searchAfterBuilder = searchAfterBuilder;
        rewrittenBuilder.sliceBuilder = slice;
        rewrittenBuilder.size = size;
        rewrittenBuilder.sorts = sorts;
        rewrittenBuilder.stats = stats;
        rewrittenBuilder.suggestBuilder = suggestBuilder;
        rewrittenBuilder.terminateAfter = terminateAfter;
        rewrittenBuilder.timeout = timeout;
        rewrittenBuilder.trackScores = trackScores;
        rewrittenBuilder.trackTotalHitsUpTo = trackTotalHitsUpTo;
        rewrittenBuilder.version = version;
        rewrittenBuilder.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
        rewrittenBuilder.collapse = collapse;
        rewrittenBuilder.pointInTimeBuilder = pointInTimeBuilder;
        rewrittenBuilder.runtimeMappings = runtimeMappings;
        return rewrittenBuilder;
    }

    public void parseXContent(XContentParser parser) throws IOException {
        parseXContent(parser, true);
    }

    /**
     * Parse some xContent into this SearchSourceBuilder, overwriting any values specified in the xContent. Use this if you need to set up
     * different defaults than a regular SearchSourceBuilder would have and use {@link #fromXContent(XContentParser, boolean)} if you have
     * normal defaults.
     *
     * @param parser The xContent parser.
     * @param checkTrailingTokens If true throws a parsing exception when extra tokens are found after the main object.
     */
    public void parseXContent(XContentParser parser, boolean checkTrailingTokens) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT +
                    "] but found [" + token + "]", parser.getTokenLocation());
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    from = parser.intValue();
                } else if (SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    int parsedSize = parser.intValue();
                    // we treat -1 as not-set, but deprecate it to be able to later remove this funny extra treatment
                    if (parsedSize != -1) {
                        size(parsedSize);
                    } else {
                        deprecationLogger.deprecate(
                            DeprecationCategory.API,
                            "search-api-size-1",
                            "Using search size of -1 is deprecated and will be removed in future versions. "
                            + "Instead, don't use the `size` parameter if you don't want to set it explicitly."
                        );
                    }

                } else if (TIMEOUT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    timeout = TimeValue.parseTimeValue(parser.text(), null, TIMEOUT_FIELD.getPreferredName());
                } else if (TERMINATE_AFTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    terminateAfter(parser.intValue());
                } else if (MIN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minScore = parser.floatValue();
                } else if (VERSION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    version = parser.booleanValue();
                } else if (SEQ_NO_PRIMARY_TERM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    seqNoAndPrimaryTerm = parser.booleanValue();
                } else if (EXPLAIN_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    explain = parser.booleanValue();
                } else if (TRACK_SCORES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    trackScores = parser.booleanValue();
                } else if (TRACK_TOTAL_HITS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.VALUE_BOOLEAN ||
                        (token == XContentParser.Token.VALUE_STRING && Booleans.isBoolean(parser.text()))) {
                        trackTotalHits(parser.booleanValue());
                    } else {
                        trackTotalHitsUpTo(parser.intValue());
                    }
                } else if (_SOURCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fetchSourceContext = FetchSourceContext.fromXContent(parser);
                } else if (STORED_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    storedFieldsContext =
                        StoredFieldsContext.fromXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), parser);
                } else if (SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sort(parser.text());
                } else if (PROFILE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    profile = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryBuilder = parseInnerQueryBuilder(parser);
                } else if (POST_FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    postQueryBuilder = parseInnerQueryBuilder(parser);
                } else if (_SOURCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fetchSourceContext = FetchSourceContext.fromXContent(parser);
                } else if (SCRIPT_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scriptFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        scriptFields.add(new ScriptField(parser));
                    }
                } else if (INDICES_BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    deprecationLogger.deprecate(DeprecationCategory.API, "indices_boost_object_format",
                        "Object format in indices_boost is deprecated, please use array format instead");
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            indexBoosts.add(new IndexBoost(currentFieldName, parser.floatValue()));
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token +
                                " in [" + currentFieldName + "].", parser.getTokenLocation());
                        }
                    }
                } else if (AGGREGATIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())
                        || AGGS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    aggregations = AggregatorFactories.parseAggregators(parser);
                } else if (HIGHLIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    highlightBuilder = HighlightBuilder.fromXContent(parser);
                } else if (SUGGEST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    suggestBuilder = SuggestBuilder.fromXContent(parser);
                } else if (SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sorts = new ArrayList<>(SortBuilder.fromXContent(parser));
                } else if (RESCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    rescoreBuilders = new ArrayList<>();
                    rescoreBuilders.add(RescorerBuilder.parseFromXContent(parser));
                } else if (EXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    extBuilders = new ArrayList<>();
                    String extSectionName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            extSectionName = parser.currentName();
                        } else {
                            SearchExtBuilder searchExtBuilder = parser.namedObject(SearchExtBuilder.class, extSectionName, null);
                            if (searchExtBuilder.getWriteableName().equals(extSectionName) == false) {
                                throw new IllegalStateException(
                                    "The parsed ["
                                        + searchExtBuilder.getClass().getName()
                                        + "] object has a different writeable name compared to the name of the section that "
                                        + " it was parsed from: found ["
                                        + searchExtBuilder.getWriteableName()
                                        + "] expected ["
                                        + extSectionName
                                        + "]"
                                );
                            }
                            extBuilders.add(searchExtBuilder);
                        }
                    }
                } else if (SLICE.match(currentFieldName, parser.getDeprecationHandler())) {
                    sliceBuilder = SliceBuilder.fromXContent(parser);
                } else if (COLLAPSE.match(currentFieldName, parser.getDeprecationHandler())) {
                    collapse = CollapseBuilder.fromXContent(parser);
                } else if (POINT_IN_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    pointInTimeBuilder = PointInTimeBuilder.fromXContent(parser);
                } else if (RUNTIME_MAPPINGS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    runtimeMappings = parser.map();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (STORED_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    storedFieldsContext = StoredFieldsContext.fromXContent(STORED_FIELDS_FIELD.getPreferredName(), parser);
                } else if (DOCVALUE_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    docValueFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        docValueFields.add(FieldAndFormat.fromXContent(parser));
                    }
                } else if (FETCH_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fetchFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fetchFields.add(FieldAndFormat.fromXContent(parser));
                    }
                } else if (INDICES_BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        indexBoosts.add(new IndexBoost(parser));
                    }
                } else if (SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sorts = new ArrayList<>(SortBuilder.fromXContent(parser));
                } else if (RESCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    rescoreBuilders = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rescoreBuilders.add(RescorerBuilder.parseFromXContent(parser));
                    }
                } else if (STATS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    stats = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            stats.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING +
                                    "] in [" + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                } else if (_SOURCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fetchSourceContext = FetchSourceContext.fromXContent(parser);
                } else if (SEARCH_AFTER.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchAfterBuilder = SearchAfterBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation());
            }
        }
        if (checkTrailingTokens) {
            token = parser.nextToken();
            if (token != null) {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token [" + token + "] found after the main object.");
            }
        }
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (from != -1) {
            builder.field(FROM_FIELD.getPreferredName(), from);
        }
        if (size != -1) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }

        if (timeout != null && timeout.equals(TimeValue.MINUS_ONE) == false) {
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

        if (seqNoAndPrimaryTerm != null) {
            builder.field(SEQ_NO_PRIMARY_TERM_FIELD.getPreferredName(), seqNoAndPrimaryTerm);
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

        if (storedFieldsContext != null) {
            storedFieldsContext.toXContent(STORED_FIELDS_FIELD.getPreferredName(), builder);
        }

        if (docValueFields != null) {
            builder.startArray(DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : docValueFields) {
               docValueField.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (fetchFields != null) {
            builder.startArray(FETCH_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : fetchFields) {
                docValueField.toXContent(builder, params);
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

        if (trackTotalHitsUpTo != null) {
            builder.field(TRACK_TOTAL_HITS_FIELD.getPreferredName(), trackTotalHitsUpTo);
        }

        if (searchAfterBuilder != null) {
            builder.array(SEARCH_AFTER.getPreferredName(), searchAfterBuilder.getSortValues());
        }

        if (sliceBuilder != null) {
            builder.field(SLICE.getPreferredName(), sliceBuilder);
        }

        if (indexBoosts.isEmpty() == false) {
            builder.startArray(INDICES_BOOST_FIELD.getPreferredName());
            for (IndexBoost ib : indexBoosts) {
                builder.startObject();
                builder.field(ib.index, ib.boost);
                builder.endObject();
            }
            builder.endArray();
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
            for (RescorerBuilder<?> rescoreBuilder : rescoreBuilders) {
                rescoreBuilder.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (stats != null) {
            builder.field(STATS_FIELD.getPreferredName(), stats);
        }

        if (extBuilders != null && extBuilders.isEmpty() == false) {
            builder.startObject(EXT_FIELD.getPreferredName());
            for (SearchExtBuilder extBuilder : extBuilders) {
                extBuilder.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (collapse != null) {
            builder.field(COLLAPSE.getPreferredName(), collapse);
        }
        if (pointInTimeBuilder != null) {
            pointInTimeBuilder.toXContent(builder, params);
        }
        if (false == runtimeMappings.isEmpty()) {
            builder.field(RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }

        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static class IndexBoost implements Writeable, ToXContentObject {
        private final String index;
        private final float boost;

        IndexBoost(String index, float boost) {
            this.index = index;
            this.boost = boost;
        }

        IndexBoost(StreamInput in) throws IOException {
            index = in.readString();
            boost = in.readFloat();
        }

        IndexBoost(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();

            if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    index = parser.currentName();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.FIELD_NAME +
                        "] in [" + INDICES_BOOST_FIELD + "] but found [" + token + "]", parser.getTokenLocation());
                }
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_NUMBER +
                        "] in [" + INDICES_BOOST_FIELD + "] but found [" + token + "]", parser.getTokenLocation());
                }
                token = parser.nextToken();
                if (token != XContentParser.Token.END_OBJECT) {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.END_OBJECT +
                        "] in [" + INDICES_BOOST_FIELD + "] but found [" + token + "]", parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT +
                    "] in [" + parser.currentName() + "] but found [" + token + "]", parser.getTokenLocation());
            }
        }

        public String getIndex() {
            return index;
        }

        public float getBoost() {
            return boost;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeFloat(boost);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(index, boost);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, boost);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            IndexBoost other = (IndexBoost) obj;
            return Objects.equals(index, other.index)
                && Objects.equals(boost, other.boost);
        }

    }

    public static class ScriptField implements Writeable, ToXContentFragment {

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

        public ScriptField(XContentParser parser) throws IOException {
            boolean ignoreFailure = false;
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
                        if (SCRIPT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            script = Script.parse(parser);
                        } else if (IGNORE_FAILURE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            ignoreFailure = parser.booleanValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                    + "].", parser.getTokenLocation());
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (SCRIPT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            script = Script.parse(parser);
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
        return Objects.hash(aggregations, explain, fetchSourceContext, fetchFields, docValueFields, storedFieldsContext, from,
            highlightBuilder, indexBoosts, minScore, postQueryBuilder, queryBuilder, rescoreBuilders, scriptFields, size,
            sorts, searchAfterBuilder, sliceBuilder, stats, suggestBuilder, terminateAfter, timeout, trackScores, version,
            seqNoAndPrimaryTerm, profile, extBuilders, collapse, trackTotalHitsUpTo, pointInTimeBuilder, runtimeMappings);
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
                && Objects.equals(fetchFields, other.fetchFields)
                && Objects.equals(docValueFields, other.docValueFields)
                && Objects.equals(storedFieldsContext, other.storedFieldsContext)
                && Objects.equals(from, other.from)
                && Objects.equals(highlightBuilder, other.highlightBuilder)
                && Objects.equals(indexBoosts, other.indexBoosts)
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
                && Objects.equals(seqNoAndPrimaryTerm, other.seqNoAndPrimaryTerm)
                && Objects.equals(profile, other.profile)
                && Objects.equals(extBuilders, other.extBuilders)
                && Objects.equals(collapse, other.collapse)
                && Objects.equals(trackTotalHitsUpTo, other.trackTotalHitsUpTo)
                && Objects.equals(pointInTimeBuilder, other.pointInTimeBuilder)
                && Objects.equals(runtimeMappings, other.runtimeMappings);
    }

    @Override
    public String toString() {
        return toString(EMPTY_PARAMS);
    }

    public String toString(Params params) {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, params, true).utf8ToString();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

}
