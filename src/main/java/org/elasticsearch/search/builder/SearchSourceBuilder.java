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

package org.elasticsearch.search.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TObjectFloatHashMap;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A search source builder allowing to easily build search source. Simple construction
 * using {@link org.elasticsearch.search.builder.SearchSourceBuilder#searchSource()}.
 *
 * @see org.elasticsearch.action.search.SearchRequest#source(SearchSourceBuilder)
 */
public class SearchSourceBuilder implements ToXContent {

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

    private BytesReference queryBinary;

    private FilterBuilder filterBuilder;

    private BytesReference filterBinary;

    private int from = -1;

    private int size = -1;

    private Boolean explain;

    private Boolean version;

    private List<SortBuilder> sorts;

    private boolean trackScores = false;

    private Float minScore;

    private long timeoutInMillis = -1;

    private List<String> fieldNames;
    private List<ScriptField> scriptFields;
    private List<PartialField> partialFields;

    private List<AbstractFacetBuilder> facets;

    private BytesReference facetsBinary;

    private HighlightBuilder highlightBuilder;

    private TObjectFloatHashMap<String> indexBoost = null;

    private String[] stats;


    /**
     * Constructs a new search source builder.
     */
    public SearchSourceBuilder() {
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public SearchSourceBuilder query(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchSourceBuilder query(byte[] queryBinary) {
        return query(queryBinary, 0, queryBinary.length);
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchSourceBuilder query(byte[] queryBinary, int queryBinaryOffset, int queryBinaryLength) {
        return query(new BytesArray(queryBinary, queryBinaryOffset, queryBinaryLength));
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchSourceBuilder query(BytesReference queryBinary) {
        this.queryBinary = queryBinary;
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchSourceBuilder query(String queryString) {
        return query(Unicode.fromStringAsBytes(queryString));
    }

    /**
     * Constructs a new search source builder with a query from a builder.
     */
    public SearchSourceBuilder query(XContentBuilder query) {
        return query(query.bytes());
    }

    /**
     * Constructs a new search source builder with a query from a map.
     */
    public SearchSourceBuilder query(Map query) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(query);
            return query(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + query + "]", e);
        }
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(FilterBuilder filter) {
        this.filterBuilder = filter;
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(String filterString) {
        return filter(Unicode.fromStringAsBytes(filterString));
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(byte[] filter) {
        return filter(filter, 0, filter.length);
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(byte[] filterBinary, int filterBinaryOffset, int filterBinaryLength) {
        return filter(new BytesArray(filterBinary, filterBinaryOffset, filterBinaryLength));
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(BytesReference filterBinary) {
        this.filterBinary = filterBinary;
        return this;
    }

    /**
     * Constructs a new search source builder with a query from a builder.
     */
    public SearchSourceBuilder filter(XContentBuilder filter) {
        return filter(filter.bytes());
    }

    /**
     * Constructs a new search source builder with a query from a map.
     */
    public SearchSourceBuilder filter(Map filter) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(filter);
            return filter(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public SearchSourceBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public SearchSourceBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public SearchSourceBuilder minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public SearchSourceBuilder explain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with a version
     * associated with it.
     */
    public SearchSourceBuilder version(Boolean version) {
        this.version = version;
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchSourceBuilder timeout(TimeValue timeout) {
        this.timeoutInMillis = timeout.millis();
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchSourceBuilder timeout(String timeout) {
        this.timeoutInMillis = TimeValue.parseTimeValue(timeout, null).millis();
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param name  The name of the field
     * @param order The sort ordering
     */
    public SearchSourceBuilder sort(String name, SortOrder order) {
        return sort(SortBuilders.fieldSort(name).order(order));
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name The name of the field to sort by
     */
    public SearchSourceBuilder sort(String name) {
        return sort(SortBuilders.fieldSort(name));
    }

    /**
     * Adds a sort builder.
     */
    public SearchSourceBuilder sort(SortBuilder sort) {
        if (sorts == null) {
            sorts = Lists.newArrayList();
        }
        sorts.add(sort);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to
     * <tt>false</tt>.
     */
    public SearchSourceBuilder trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Add a facet to perform as part of the search.
     */
    public SearchSourceBuilder facet(AbstractFacetBuilder facet) {
        if (facets == null) {
            facets = Lists.newArrayList();
        }
        facets.add(facet);
        return this;
    }

    /**
     * Sets a raw (xcontent / json) facets.
     */
    public SearchSourceBuilder facets(byte[] facetsBinary) {
        return facets(facetsBinary, 0, facetsBinary.length);
    }

    /**
     * Sets a raw (xcontent / json) facets.
     */
    public SearchSourceBuilder facets(byte[] facetsBinary, int facetBinaryOffset, int facetBinaryLength) {
        return facets(new BytesArray(facetsBinary, facetBinaryOffset, facetBinaryLength));
    }

    /**
     * Sets a raw (xcontent / json) facets.
     */
    public SearchSourceBuilder facets(BytesReference facetsBinary) {
        this.facetsBinary = facetsBinary;
        return this;
    }

    /**
     * Sets a raw (xcontent / json) facets.
     */
    public SearchSourceBuilder facets(XContentBuilder facets) {
        return facets(facets.bytes());
    }

    /**
     * Sets a raw (xcontent / json) facets.
     */
    public SearchSourceBuilder facets(Map facets) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(facets);
            return facets(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + facets + "]", e);
        }
    }

    public HighlightBuilder highlighter() {
        if (highlightBuilder == null) {
            highlightBuilder = new HighlightBuilder();
        }
        return highlightBuilder;
    }

    /**
     * Adds highlight to perform as part of the search.
     */
    public SearchSourceBuilder highlight(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be returned per field.
     */
    public SearchSourceBuilder noFields() {
        this.fieldNames = ImmutableList.of();
        return this;
    }

    /**
     * Sets the fields to load and return as part of the search request. If none are specified,
     * the source of the document will be returned.
     */
    public SearchSourceBuilder fields(List<String> fields) {
        this.fieldNames = fields;
        return this;
    }

    /**
     * Adds the fields to load and return as part of the search request. If none are specified,
     * the source of the document will be returned.
     */
    public SearchSourceBuilder fields(String... fields) {
        if (fieldNames == null) {
            fieldNames = new ArrayList<String>();
        }
        for (String field : fields) {
            fieldNames.add(field);
        }
        return this;
    }

    /**
     * Adds a field to load and return (note, it must be stored) as part of the search request.
     * If none are specified, the source of the document will be return.
     */
    public SearchSourceBuilder field(String name) {
        if (fieldNames == null) {
            fieldNames = new ArrayList<String>();
        }
        fieldNames.add(name);
        return this;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name   The name of the field
     * @param script The script
     */
    public SearchSourceBuilder scriptField(String name, String script) {
        return scriptField(name, null, script, null);
    }

    /**
     * Adds a script field.
     *
     * @param name   The name of the field
     * @param script The script to execute
     * @param params The script parameters
     */
    public SearchSourceBuilder scriptField(String name, String script, Map<String, Object> params) {
        return scriptField(name, null, script, params);
    }

    /**
     * Adds a script field.
     *
     * @param name   The name of the field
     * @param lang   The language of the script
     * @param script The script to execute
     * @param params The script parameters (can be <tt>null</tt>)
     * @return
     */
    public SearchSourceBuilder scriptField(String name, String lang, String script, Map<String, Object> params) {
        if (scriptFields == null) {
            scriptFields = Lists.newArrayList();
        }
        scriptFields.add(new ScriptField(name, lang, script, params));
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
    public SearchSourceBuilder partialField(String name, @Nullable String include, @Nullable String exclude) {
        if (partialFields == null) {
            partialFields = Lists.newArrayList();
        }
        partialFields.add(new PartialField(name, include, exclude));
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
    public SearchSourceBuilder partialField(String name, @Nullable String[] includes, @Nullable String[] excludes) {
        if (partialFields == null) {
            partialFields = Lists.newArrayList();
        }
        partialFields.add(new PartialField(name, includes, excludes));
        return this;
    }

    /**
     * Sets the boost a specific index will receive when the query is executeed against it.
     *
     * @param index      The index to apply the boost against
     * @param indexBoost The boost to apply to the index
     */
    public SearchSourceBuilder indexBoost(String index, float indexBoost) {
        if (this.indexBoost == null) {
            this.indexBoost = new TObjectFloatHashMap<String>();
        }
        this.indexBoost.put(index, indexBoost);
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public SearchSourceBuilder stats(String... statsGroups) {
        this.stats = statsGroups;
        return this;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public BytesReference buildAsBytes() throws SearchSourceBuilderException {
        return buildAsBytes(Requests.CONTENT_TYPE);
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

        if (from != -1) {
            builder.field("from", from);
        }
        if (size != -1) {
            builder.field("size", size);
        }

        if (timeoutInMillis != -1) {
            builder.field("timeout", timeoutInMillis);
        }

        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }

        if (queryBinary != null) {
            if (XContentFactory.xContentType(queryBinary) == builder.contentType()) {
                builder.rawField("query", queryBinary);
            } else {
                builder.field("query_binary", queryBinary);
            }
        }

        if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }

        if (filterBinary != null) {
            if (XContentFactory.xContentType(filterBinary) == builder.contentType()) {
                builder.rawField("filter", filterBinary);
            } else {
                builder.field("filter_binary", filterBinary);
            }
        }

        if (minScore != null) {
            builder.field("min_score", minScore);
        }

        if (version != null) {
            builder.field("version", version);
        }

        if (explain != null) {
            builder.field("explain", explain);
        }

        if (fieldNames != null) {
            if (fieldNames.size() == 1) {
                builder.field("fields", fieldNames.get(0));
            } else {
                builder.startArray("fields");
                for (String fieldName : fieldNames) {
                    builder.value(fieldName);
                }
                builder.endArray();
            }
        }

        if (partialFields != null) {
            builder.startObject("partial_fields");
            for (PartialField partialField : partialFields) {
                builder.startObject(partialField.name());
                if (partialField.includes() != null) {
                    if (partialField.includes().length == 1) {
                        builder.field("include", partialField.includes()[0]);
                    } else {
                        builder.field("include", partialField.includes());
                    }
                }
                if (partialField.excludes() != null) {
                    if (partialField.excludes().length == 1) {
                        builder.field("exclude", partialField.excludes()[0]);
                    } else {
                        builder.field("exclude", partialField.excludes());
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        }

        if (scriptFields != null) {
            builder.startObject("script_fields");
            for (ScriptField scriptField : scriptFields) {
                builder.startObject(scriptField.fieldName());
                builder.field("script", scriptField.script());
                if (scriptField.lang() != null) {
                    builder.field("lang", scriptField.lang());
                }
                if (scriptField.params() != null) {
                    builder.field("params");
                    builder.map(scriptField.params());
                }
                builder.endObject();
            }
            builder.endObject();
        }

        if (sorts != null) {
            builder.startArray("sort");
            for (SortBuilder sort : sorts) {
                builder.startObject();
                sort.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            if (trackScores) {
                builder.field("track_scores", trackScores);
            }
        }

        if (indexBoost != null) {
            builder.startObject("indices_boost");
            for (TObjectFloatIterator<String> it = indexBoost.iterator(); it.hasNext(); ) {
                it.advance();
                builder.field(it.key(), it.value());
            }
            builder.endObject();
        }

        if (facets != null) {
            builder.field("facets");
            builder.startObject();
            for (AbstractFacetBuilder facet : facets) {
                facet.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (facetsBinary != null) {
            if (XContentFactory.xContentType(facetsBinary) == builder.contentType()) {
                builder.rawField("facets", facetsBinary);
            } else {
                builder.field("facets_binary", facetsBinary);
            }
        }

        if (highlightBuilder != null) {
            highlightBuilder.toXContent(builder, params);
        }

        if (stats != null) {
            builder.startArray("stats");
            for (String stat : stats) {
                builder.value(stat);
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    private static class ScriptField {
        private final String fieldName;
        private final String script;
        private final String lang;
        private final Map<String, Object> params;

        private ScriptField(String fieldName, String lang, String script, Map<String, Object> params) {
            this.fieldName = fieldName;
            this.lang = lang;
            this.script = script;
            this.params = params;
        }

        public String fieldName() {
            return fieldName;
        }

        public String script() {
            return script;
        }

        public String lang() {
            return this.lang;
        }

        public Map<String, Object> params() {
            return params;
        }
    }

    private static class PartialField {
        private final String name;
        private final String[] includes;
        private final String[] excludes;

        private PartialField(String name, String[] includes, String[] excludes) {
            this.name = name;
            this.includes = includes;
            this.excludes = excludes;
        }

        private PartialField(String name, String include, String exclude) {
            this.name = name;
            this.includes = include == null ? null : new String[]{include};
            this.excludes = exclude == null ? null : new String[]{exclude};
        }

        public String name() {
            return name;
        }

        public String[] includes() {
            return includes;
        }

        public String[] excludes() {
            return excludes;
        }
    }
}
