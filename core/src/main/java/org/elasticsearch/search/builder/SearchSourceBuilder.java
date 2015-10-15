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
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;

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
public final class SearchSourceBuilder extends ToXContentToBytes implements Writeable<SearchSourceBuilder> {

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
    public static final ParseField FIELDDATA_FIELDS_FIELD = new ParseField("fielddata_fields");
    public static final ParseField SCRIPT_FIELDS_FIELD = new ParseField("script_fields");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField IGNORE_FAILURE_FIELD = new ParseField("ignore_failure");
    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField TRACK_SCORES_FIELD = new ParseField("track_scores");
    public static final ParseField INDICES_BOOST_FIELD = new ParseField("indices_boost");
    public static final ParseField AGGREGATIONS_FIELD = new ParseField("aggregations", "aggs");
    public static final ParseField HIGHLIGHT_FIELD = new ParseField("highlight");
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final ParseField SUGGEST_FIELD = new ParseField("suggest");
    public static final ParseField RESCORE_FIELD = new ParseField("rescore");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField EXT_FIELD = new ParseField("ext");

    private static final SearchSourceBuilder PROTOTYPE = new SearchSourceBuilder();

    public static SearchSourceBuilder readSearchSourceFrom(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    public static SearchSourceBuilder parseSearchSource(XContentParser parser, QueryParseContext context) throws IOException {
        return PROTOTYPE.fromXContent(parser, context);
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

    private QueryBuilder<?> queryBuilder;

    private QueryBuilder<?> postQueryBuilder;

    private int from = -1;

    private int size = -1;

    private Boolean explain;

    private Boolean version;

    private List<BytesReference> sorts;

    private boolean trackScores = false;

    private Float minScore;

    private long timeoutInMillis = -1;
    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;

    private List<String> fieldNames;
    private List<String> fieldDataFields;
    private List<ScriptField> scriptFields;
    private FetchSourceContext fetchSourceContext;

    private List<BytesReference> aggregations;

    private BytesReference highlightBuilder;

    private BytesReference suggestBuilder;

    private BytesReference innerHitsBuilder;

    private List<BytesReference> rescoreBuilders;

    private ObjectFloatHashMap<String> indexBoost = null;

    private List<String> stats;

    private BytesReference ext = null;

    /**
     * Constructs a new search source builder.
     */
    public SearchSourceBuilder() {
    }

    /**
     * Sets the search query for this request.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public SearchSourceBuilder query(QueryBuilder<?> query) {
        this.queryBuilder = query;
        return this;
    }

    /**
     * Gets the query for this request
     */
    public QueryBuilder<?> query() {
        return queryBuilder;
    }

    /**
     * Sets a filter that will be executed after the query has been executed and
     * only has affect on the search hits (not aggregations). This filter is
     * always executed as last filtering mechanism.
     */
    public SearchSourceBuilder postFilter(QueryBuilder<?> postFilter) {
        this.postQueryBuilder = postFilter;
        return this;
    }

    /**
     * Gets the post filter for this request
     */
    public QueryBuilder<?> postFilter() {
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
        this.timeoutInMillis = timeout.millis();
        return this;
    }

    /**
     * Gets the timeout to control how long search is allowed to take.
     */
    public long timeoutInMillis() {
        return timeoutInMillis;
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
        return sort(SortBuilders.fieldSort(name).order(order));
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name
     *            The name of the field to sort by
     */
    public SearchSourceBuilder sort(String name) {
        return sort(SortBuilders.fieldSort(name));
    }

    /**
     * Adds a sort builder.
     */
    public SearchSourceBuilder sort(SortBuilder sort) {
        try {
            if (sorts == null) {
                sorts = new ArrayList<>();
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            sort.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            sorts.add(builder.bytes());
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the bytes representing the sort builders for this request.
     */
    public List<BytesReference> sorts() {
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
     * Add an aggregation to perform as part of the search.
     */
    public SearchSourceBuilder aggregation(AbstractAggregationBuilder aggregation) {
        try {
            if (aggregations == null) {
                aggregations = new ArrayList<>();
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            aggregation.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            aggregations.add(builder.bytes());
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the bytes representing the aggregation builders for this request.
     */
    public List<BytesReference> aggregations() {
        return aggregations;
    }

    /**
     * Adds highlight to perform as part of the search.
     */
    public SearchSourceBuilder highlighter(HighlightBuilder highlightBuilder) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            highlightBuilder.innerXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            this.highlightBuilder = builder.bytes();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the bytes representing the hightlighter builder for this request.
     */
    public BytesReference highlighter() {
        return highlightBuilder;
    }

    public SearchSourceBuilder innerHits(InnerHitsBuilder innerHitsBuilder) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            innerHitsBuilder.innerXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            this.innerHitsBuilder = builder.bytes();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the bytes representing the inner hits builder for this request.
     */
    public BytesReference innerHits() {
        return innerHitsBuilder;
    }

    public SearchSourceBuilder suggest(SuggestBuilder suggestBuilder) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            suggestBuilder.toXContent(builder, EMPTY_PARAMS);
            this.suggestBuilder = builder.bytes();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the bytes representing the suggester builder for this request.
     */
    public BytesReference suggest() {
        return suggestBuilder;
    }

    public SearchSourceBuilder addRescorer(RescoreBuilder rescoreBuilder) {
        try {
            if (rescoreBuilders == null) {
                rescoreBuilders = new ArrayList<>();
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            rescoreBuilder.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            rescoreBuilders.add(builder.bytes());
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SearchSourceBuilder clearRescorers() {
        rescoreBuilders = null;
        return this;
    }

    /**
     * Gets the bytes representing the rescore builders for this request.
     */
    public List<BytesReference> rescores() {
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
     * Adds a field to load and return (note, it must be stored) as part of the
     * search request. If none are specified, the source of the document will be
     * return.
     */
    public SearchSourceBuilder field(String name) {
        if (fieldNames == null) {
            fieldNames = new ArrayList<>();
        }
        fieldNames.add(name);
        return this;
    }

    /**
     * Sets the fields to load and return as part of the search request. If none
     * are specified, the source of the document will be returned.
     */
    public SearchSourceBuilder fields(List<String> fields) {
        this.fieldNames = fields;
        return this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be returned
     * per field.
     */
    public SearchSourceBuilder noFields() {
        this.fieldNames = Collections.emptyList();
        return this;
    }

    /**
     * Gets the fields to load and return as part of the search request.
     */
    public List<String> fields() {
        return fieldNames;
    }

    /**
     * Adds a field to load from the field data cache and return as part of the
     * search request.
     */
    public SearchSourceBuilder fieldDataField(String name) {
        if (fieldDataFields == null) {
            fieldDataFields = new ArrayList<>();
        }
        fieldDataFields.add(name);
        return this;
    }

    /**
     * Gets the field-data fields.
     */
    public List<String> fieldDataFields() {
        return fieldDataFields;
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
     * Sets the boost a specific index will receive when the query is executeed
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
     * executeed against them.
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

    public SearchSourceBuilder fromXContent(XContentParser parser, QueryParseContext context) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
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
                if (context.parseFieldMatcher().match(currentFieldName, FROM_FIELD)) {
                    builder.from = parser.intValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, SIZE_FIELD)) {
                    builder.size = parser.intValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, TIMEOUT_FIELD)) {
                    builder.timeoutInMillis = parser.longValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, TERMINATE_AFTER_FIELD)) {
                    builder.terminateAfter = parser.intValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, MIN_SCORE_FIELD)) {
                    builder.minScore = parser.floatValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, VERSION_FIELD)) {
                    builder.version = parser.booleanValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, EXPLAIN_FIELD)) {
                    builder.explain = parser.booleanValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, TRACK_SCORES_FIELD)) {
                    builder.trackScores = parser.booleanValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    FetchSourceContext fetchSourceContext = FetchSourceContext.parse(parser, context);
                    builder.fetchSourceContext = fetchSourceContext;
                } else if (context.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    List<String> fieldNames = new ArrayList<>();
                    fieldNames.add(parser.text());
                    builder.fieldNames = fieldNames;
                } else if (context.parseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    builder.sort(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    builder.queryBuilder = context.parseInnerQueryBuilder();
                } else if (context.parseFieldMatcher().match(currentFieldName, POST_FILTER_FIELD)) {
                    builder.postQueryBuilder = context.parseInnerQueryBuilder();
                } else if (context.parseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    FetchSourceContext fetchSourceContext = FetchSourceContext.parse(parser, context);
                    builder.fetchSourceContext = fetchSourceContext;
                } else if (context.parseFieldMatcher().match(currentFieldName, SCRIPT_FIELDS_FIELD)) {
                    List<ScriptField> scriptFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        String scriptFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_OBJECT) {
                            Script script = null;
                            boolean ignoreFailure = false;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (context.parseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                                        script = Script.parse(parser, context.parseFieldMatcher());
                                    } else if (context.parseFieldMatcher().match(currentFieldName, IGNORE_FAILURE_FIELD)) {
                                        ignoreFailure = parser.booleanValue();
                                    } else {
                                        throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                                + "].", parser.getTokenLocation());
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if (context.parseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                                        script = Script.parse(parser, context.parseFieldMatcher());
                                    } else {
                                        throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                                + "].", parser.getTokenLocation());
                                    }
                                } else {
                                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName
                                            + "].", parser.getTokenLocation());
                                }
                            }
                            scriptFields.add(new ScriptField(scriptFieldName, script, ignoreFailure));
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    builder.scriptFields = scriptFields;
                } else if (context.parseFieldMatcher().match(currentFieldName, INDICES_BOOST_FIELD)) {
                    ObjectFloatHashMap<String> indexBoost = new ObjectFloatHashMap<String>();
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
                    builder.indexBoost = indexBoost;
                } else if (context.parseFieldMatcher().match(currentFieldName, AGGREGATIONS_FIELD)) {
                    List<BytesReference> aggregations = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_OBJECT) {
                            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType());
                            xContentBuilder.startObject();
                            xContentBuilder.field(currentFieldName);
                            xContentBuilder.copyCurrentStructure(parser);
                            xContentBuilder.endObject();
                            aggregations.add(xContentBuilder.bytes());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                    parser.getTokenLocation());
                        }
                    }
                    builder.aggregations = aggregations;
                } else if (context.parseFieldMatcher().match(currentFieldName, HIGHLIGHT_FIELD)) {
                    XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                    builder.highlightBuilder = xContentBuilder.bytes();
                } else if (context.parseFieldMatcher().match(currentFieldName, INNER_HITS_FIELD)) {
                    XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                    builder.innerHitsBuilder = xContentBuilder.bytes();
                } else if (context.parseFieldMatcher().match(currentFieldName, SUGGEST_FIELD)) {
                    XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType());
                    xContentBuilder.copyCurrentStructure(parser);
                    builder.suggestBuilder = xContentBuilder.bytes();
                } else if (context.parseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    List<BytesReference> sorts = new ArrayList<>();
                    XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                    sorts.add(xContentBuilder.bytes());
                    builder.sorts = sorts;
                } else if (context.parseFieldMatcher().match(currentFieldName, EXT_FIELD)) {
                    XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                    builder.ext = xContentBuilder.bytes();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {

                if (context.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    List<String> fieldNames = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            fieldNames.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    builder.fieldNames = fieldNames;
                } else if (context.parseFieldMatcher().match(currentFieldName, FIELDDATA_FIELDS_FIELD)) {
                    List<String> fieldDataFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            fieldDataFields.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    builder.fieldDataFields = fieldDataFields;
                } else if (context.parseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    List<BytesReference> sorts = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                        sorts.add(xContentBuilder.bytes());
                    }
                    builder.sorts = sorts;
                } else if (context.parseFieldMatcher().match(currentFieldName, RESCORE_FIELD)) {
                    List<BytesReference> rescoreBuilders = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                        rescoreBuilders.add(xContentBuilder.bytes());
                    }
                    builder.rescoreBuilders = rescoreBuilders;
                } else if (context.parseFieldMatcher().match(currentFieldName, STATS_FIELD)) {
                    List<String> stats = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            stats.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in ["
                                    + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                    builder.stats = stats;
                } else if (context.parseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    FetchSourceContext fetchSourceContext = FetchSourceContext.parse(parser, context);
                    builder.fetchSourceContext = fetchSourceContext;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation());
            }
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

    public void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (from != -1) {
            builder.field(FROM_FIELD.getPreferredName(), from);
        }
        if (size != -1) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }

        if (timeoutInMillis != -1) {
            builder.field(TIMEOUT_FIELD.getPreferredName(), timeoutInMillis);
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

        if (fetchSourceContext != null) {
            builder.field(_SOURCE_FIELD.getPreferredName(), fetchSourceContext);
        }

        if (fieldNames != null) {
            if (fieldNames.size() == 1) {
                builder.field(FIELDS_FIELD.getPreferredName(), fieldNames.get(0));
            } else {
                builder.startArray(FIELDS_FIELD.getPreferredName());
                for (String fieldName : fieldNames) {
                    builder.value(fieldName);
                }
                builder.endArray();
            }
        }

        if (fieldDataFields != null) {
            builder.startArray(FIELDDATA_FIELDS_FIELD.getPreferredName());
            for (String fieldDataField : fieldDataFields) {
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
            for (BytesReference sort : sorts) {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(sort);
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }
            builder.endArray();
        }

        if (trackScores) {
            builder.field(TRACK_SCORES_FIELD.getPreferredName(), true);
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
            builder.field(AGGREGATIONS_FIELD.getPreferredName());
            builder.startObject();
            for (BytesReference aggregation : aggregations) {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(aggregation);
                parser.nextToken();
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }
            builder.endObject();
        }

        if (highlightBuilder != null) {
            builder.field(HIGHLIGHT_FIELD.getPreferredName());
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(highlightBuilder);
            parser.nextToken();
            builder.copyCurrentStructure(parser);
        }

        if (innerHitsBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName());
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(innerHitsBuilder);
            parser.nextToken();
            builder.copyCurrentStructure(parser);
        }

        if (suggestBuilder != null) {
            builder.field(SUGGEST_FIELD.getPreferredName());
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(suggestBuilder);
            parser.nextToken();
            builder.copyCurrentStructure(parser);
        }

        if (rescoreBuilders != null) {
            builder.startArray(RESCORE_FIELD.getPreferredName());
            for (BytesReference rescoreBuilder : rescoreBuilders) {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(rescoreBuilder);
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }
            builder.endArray();
        }

        if (stats != null) {
            builder.field(STATS_FIELD.getPreferredName(), stats);
        }

        if (ext != null) {
            builder.field(EXT_FIELD.getPreferredName());
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(ext);
            parser.nextToken();
            builder.copyCurrentStructure(parser);
        }
    }

    public static class ScriptField implements Writeable<ScriptField>, ToXContent {

        public static final ScriptField PROTOTYPE = new ScriptField(null, null);

        private final boolean ignoreFailure;
        private final String fieldName;
        private final Script script;

        private ScriptField(String fieldName, Script script) {
            this(fieldName, script, false);
        }

        private ScriptField(String fieldName, Script script, boolean ignoreFailure) {
            this.fieldName = fieldName;
            this.script = script;
            this.ignoreFailure = ignoreFailure;
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
        public ScriptField readFrom(StreamInput in) throws IOException {
            return new ScriptField(in.readString(), Script.readScript(in), in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            script.writeTo(out);
            out.writeBoolean(ignoreFailure);
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
    public SearchSourceBuilder readFrom(StreamInput in) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<BytesReference> aggregations = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                aggregations.add(in.readBytesReference());
            }
            builder.aggregations = aggregations;
        }
        builder.explain = in.readOptionalBoolean();
        builder.fetchSourceContext = FetchSourceContext.optionalReadFromStream(in);
        boolean hasFieldDataFields = in.readBoolean();
        if (hasFieldDataFields) {
            int size = in.readVInt();
            List<String> fieldDataFields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                fieldDataFields.add(in.readString());
            }
            builder.fieldDataFields = fieldDataFields;
        }
        boolean hasFieldNames = in.readBoolean();
        if (hasFieldNames) {
            int size = in.readVInt();
            List<String> fieldNames = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                fieldNames.add(in.readString());
            }
            builder.fieldNames = fieldNames;
        }
        builder.from = in.readVInt();
        if (in.readBoolean()) {
            builder.highlightBuilder = in.readBytesReference();
        }
        boolean hasIndexBoost = in.readBoolean();
        if (hasIndexBoost) {
            int size = in.readVInt();
            ObjectFloatHashMap<String> indexBoost = new ObjectFloatHashMap<String>(size);
            for (int i = 0; i < size; i++) {
                indexBoost.put(in.readString(), in.readFloat());
            }
            builder.indexBoost = indexBoost;
        }
        if (in.readBoolean()) {
            builder.innerHitsBuilder = in.readBytesReference();
        }
        if (in.readBoolean()) {
            builder.minScore = in.readFloat();
        }
        if (in.readBoolean()) {
            builder.postQueryBuilder = in.readQuery();
        }
        if (in.readBoolean()) {
            builder.queryBuilder = in.readQuery();
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<BytesReference> rescoreBuilders = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                rescoreBuilders.add(in.readBytesReference());
            }
            builder.rescoreBuilders = rescoreBuilders;
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<ScriptField> scriptFields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(ScriptField.PROTOTYPE.readFrom(in));
            }
            builder.scriptFields = scriptFields;
        }
        builder.size = in.readVInt();
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<BytesReference> sorts = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                sorts.add(in.readBytesReference());
            }
            builder.sorts = sorts;
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<String> stats = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                stats.add(in.readString());
            }
            builder.stats = stats;
        }
        if (in.readBoolean()) {
            builder.suggestBuilder = in.readBytesReference();
        }
        builder.terminateAfter = in.readVInt();
        builder.timeoutInMillis = in.readLong();
        builder.trackScores = in.readBoolean();
        builder.version = in.readOptionalBoolean();
        if (in.readBoolean()) {
            builder.ext = in.readBytesReference();
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean hasAggregations = aggregations != null;
        out.writeBoolean(hasAggregations);
        if (hasAggregations) {
            out.writeVInt(aggregations.size());
            for (BytesReference aggregation : aggregations) {
                out.writeBytesReference(aggregation);
            }
        }
        out.writeOptionalBoolean(explain);
        FetchSourceContext.optionalWriteToStream(fetchSourceContext, out);
        boolean hasFieldDataFields = fieldDataFields != null;
        out.writeBoolean(hasFieldDataFields);
        if (hasFieldDataFields) {
            out.writeVInt(fieldDataFields.size());
            for (String field : fieldDataFields) {
                out.writeString(field);
            }
        }
        boolean hasFieldNames = fieldNames != null;
        out.writeBoolean(hasFieldNames);
        if (hasFieldNames) {
            out.writeVInt(fieldNames.size());
            for (String field : fieldNames) {
                out.writeString(field);
            }
        }
        out.writeVInt(from);
        boolean hasHighlightBuilder = highlightBuilder != null;
        out.writeBoolean(hasHighlightBuilder);
        if (hasHighlightBuilder) {
            out.writeBytesReference(highlightBuilder);
        }
        boolean hasIndexBoost = indexBoost != null;
        out.writeBoolean(hasIndexBoost);
        if (hasIndexBoost) {
            out.writeVInt(indexBoost.size());
            for (ObjectCursor<String> key : indexBoost.keys()) {
                out.writeString(key.value);
                out.writeFloat(indexBoost.get(key.value));
            }
        }
        boolean hasInnerHitsBuilder = innerHitsBuilder != null;
        out.writeBoolean(hasInnerHitsBuilder);
        if (hasInnerHitsBuilder) {
            out.writeBytesReference(innerHitsBuilder);
        }
        boolean hasMinScore = minScore != null;
        out.writeBoolean(hasMinScore);
        if (hasMinScore) {
            out.writeFloat(minScore);
        }
        boolean hasPostQuery = postQueryBuilder != null;
        out.writeBoolean(hasPostQuery);
        if (hasPostQuery) {
            out.writeQuery(postQueryBuilder);
        }
        boolean hasQuery = queryBuilder != null;
        out.writeBoolean(hasQuery);
        if (hasQuery) {
            out.writeQuery(queryBuilder);
        }
        boolean hasRescoreBuilders = rescoreBuilders != null;
        out.writeBoolean(hasRescoreBuilders);
        if (hasRescoreBuilders) {
            out.writeVInt(rescoreBuilders.size());
            for (BytesReference rescoreBuilder : rescoreBuilders) {
                out.writeBytesReference(rescoreBuilder);
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
            for (BytesReference sort : sorts) {
                out.writeBytesReference(sort);
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
        boolean hasSuggestBuilder = suggestBuilder != null;
        out.writeBoolean(hasSuggestBuilder);
        if (hasSuggestBuilder) {
            out.writeBytesReference(suggestBuilder);
        }
        out.writeVInt(terminateAfter);
        out.writeLong(timeoutInMillis);
        out.writeBoolean(trackScores);
        out.writeOptionalBoolean(version);
        boolean hasExt = ext != null;
        out.writeBoolean(hasExt);
        if (hasExt) {
            out.writeBytesReference(ext);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregations, explain, fetchSourceContext, fieldDataFields, fieldNames, from,
                highlightBuilder, indexBoost, innerHitsBuilder, minScore, postQueryBuilder, queryBuilder, rescoreBuilders, scriptFields,
                size, sorts, stats, suggestBuilder, terminateAfter, timeoutInMillis, trackScores, version);
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
                && Objects.equals(fieldDataFields, other.fieldDataFields)
                && Objects.equals(fieldNames, other.fieldNames)
                && Objects.equals(from, other.from)
                && Objects.equals(highlightBuilder, other.highlightBuilder)
                && Objects.equals(indexBoost, other.indexBoost)
                && Objects.equals(innerHitsBuilder, other.innerHitsBuilder)
                && Objects.equals(minScore, other.minScore)
                && Objects.equals(postQueryBuilder, other.postQueryBuilder)
                && Objects.equals(queryBuilder, other.queryBuilder)
                && Objects.equals(rescoreBuilders, other.rescoreBuilders)
                && Objects.equals(scriptFields, other.scriptFields)
                && Objects.equals(size, other.size)
                && Objects.equals(sorts, other.sorts)
                && Objects.equals(stats, other.stats)
                && Objects.equals(suggestBuilder, other.suggestBuilder)
                && Objects.equals(terminateAfter, other.terminateAfter)
                && Objects.equals(timeoutInMillis, other.timeoutInMillis)
                && Objects.equals(trackScores, other.trackScores)
                && Objects.equals(version, other.version);
    }
}
