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

package org.elasticsearch.search.builder;

import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.trove.iterator.TObjectFloatIterator;
import org.elasticsearch.common.trove.map.hash.TObjectFloatHashMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
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
 * @author kimchy (shay.banon)
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

    private XContentQueryBuilder queryBuilder;

    private byte[] queryBinary;

    private XContentFilterBuilder filterBuilder;

    private byte[] filterBinary;

    private int from = -1;

    private int size = -1;

    private String queryParserName;

    private Boolean explain;

    private Boolean version;

    private List<SortBuilder> sorts;

    private boolean trackScores = false;

    private Float minScore;

    private List<String> fieldNames;

    private List<ScriptField> scriptFields;

    private List<AbstractFacetBuilder> facets;

    private byte[] facetsBinary;

    private HighlightBuilder highlightBuilder;

    private TObjectFloatHashMap<String> indexBoost = null;


    /**
     * Constructs a new search source builder.
     */
    public SearchSourceBuilder() {
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.xcontent.QueryBuilders
     */
    public SearchSourceBuilder query(XContentQueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchSourceBuilder query(byte[] queryBinary) {
        this.queryBinary = queryBinary;
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public SearchSourceBuilder query(String queryString) {
        this.queryBinary = Unicode.fromStringAsBytes(queryString);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(XContentFilterBuilder filter) {
        this.filterBuilder = filter;
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(String filterString) {
        this.filterBinary = Unicode.fromStringAsBytes(filterString);
        return this;
    }

    /**
     * Sets a filter on the query executed that only applies to the search query
     * (and not facets for example).
     */
    public SearchSourceBuilder filter(byte[] filter) {
        this.filterBinary = filter;
        return this;
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
     * An optional query parser name to use.
     */
    public SearchSourceBuilder queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
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
        this.facetsBinary = facetsBinary;
        return this;
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

    public FastByteArrayOutputStream buildAsUnsafeBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.unsafeStream();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }

    public byte[] buildAsBytes() throws SearchSourceBuilderException {
        return buildAsBytes(Requests.CONTENT_TYPE);
    }

    public byte[] buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, EMPTY_PARAMS);
            return builder.copiedBytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }


    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (from != -1) {
            builder.field("from", from);
        }
        if (size != -1) {
            builder.field("size", size);
        }
        if (queryParserName != null) {
            builder.field("query_parser_name", queryParserName);
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
            if (XContentFactory.xContentType(queryBinary) == builder.contentType()) {
                builder.rawField("filter", filterBinary);
            } else {
                builder.field("filter_binary", queryBinary);
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
            for (TObjectFloatIterator<String> it = indexBoost.iterator(); it.hasNext();) {
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
}
