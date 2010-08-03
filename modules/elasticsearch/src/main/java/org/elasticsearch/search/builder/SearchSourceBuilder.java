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

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.trove.TObjectFloatHashMap;
import org.elasticsearch.common.trove.TObjectFloatIterator;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.builder.BinaryXContentBuilder;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.search.facets.AbstractFacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.query.SortParseElement;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * A search source builder allowing to easily build search source. Simple construction
 * using {@link org.elasticsearch.search.builder.SearchSourceBuilder#searchSource()}.
 *
 * @author kimchy (shay.banon)
 * @see org.elasticsearch.action.search.SearchRequest#source(SearchSourceBuilder)
 */
public class SearchSourceBuilder implements ToXContent {

    public static enum Order {
        ASC,
        DESC
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

    private XContentQueryBuilder queryBuilder;

    private byte[] queryBinary;

    private int from = -1;

    private int size = -1;

    private String queryParserName;

    private Boolean explain;

    private List<SortTuple> sortFields;

    private List<ScriptSortTuple> sortScripts;

    private List<String> fieldNames;

    private List<ScriptField> scriptFields;

    private List<AbstractFacetBuilder> facets;

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
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param name  The name of the field
     * @param order The sort ordering
     */
    public SearchSourceBuilder sort(String name, Order order) {
        boolean reverse = false;
        if (name.equals(SortParseElement.SCORE_FIELD_NAME)) {
            if (order == Order.ASC) {
                reverse = true;
            }
        } else {
            if (order == Order.DESC) {
                reverse = true;
            }
        }
        return sort(name, reverse);
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name The name of the field to sort by
     */
    public SearchSourceBuilder sort(String name) {
        return sort(name, false);
    }

    /**
     * Adds a sort script.
     *
     * @param script The script to execute.
     * @param type   The type of the result (can either be "string" or "number").
     * @param order  The order.
     * @param params Optional parameters to the script.
     */
    public SearchSourceBuilder sortScript(String script, String type, Order order, @Nullable Map<String, Object> params) {
        if (sortScripts == null) {
            sortScripts = Lists.newArrayList();
        }
        sortScripts.add(new ScriptSortTuple(script, type, params, order == Order.DESC));
        return this;
    }

    /**
     * Add a sort against the given field name and if it should be revered or not.
     *
     * @param name    The name of the field to sort by
     * @param reverse Should the sort be reversed or not
     */
    public SearchSourceBuilder sort(String name, boolean reverse) {
        if (sortFields == null) {
            sortFields = newArrayListWithCapacity(2);
        }
        sortFields.add(new SortTuple(name, reverse));
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
     * Adds highlight to perform as part of the search.
     */
    public SearchSourceBuilder highlight(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
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

    public SearchSourceBuilder scriptField(String name, String script) {
        return scriptField(name, script, null);
    }

    public SearchSourceBuilder scriptField(String name, String script, Map<String, Object> params) {
        if (scriptFields == null) {
            scriptFields = Lists.newArrayList();
        }
        scriptFields.add(new ScriptField(name, script, params));
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

    public FastByteArrayOutputStream buildAsUnsafeBytes() throws SearchSourceBuilderException {
        return buildAsUnsafeBytes(XContentType.JSON);
    }

    public FastByteArrayOutputStream buildAsUnsafeBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.unsafeStream();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }

    public byte[] buildAsBytes() throws SearchSourceBuilderException {
        return buildAsBytes(XContentType.JSON);
    }

    public byte[] buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBinaryBuilder(contentType);
            toXContent(builder, EMPTY_PARAMS);
            return builder.copiedBytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }


    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
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
                builder.field("query_binary");
                builder.value(queryBinary);
            }
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
                if (scriptField.params() != null) {
                    builder.field("params");
                    builder.map(scriptField.params());
                }
                builder.endObject();
            }
            builder.endObject();
        }

        if (sortFields != null || sortScripts != null) {
            builder.field("sort");
            builder.startObject();
            if (sortFields != null) {
                for (SortTuple sortTuple : sortFields) {
                    builder.field(sortTuple.fieldName());
                    builder.startObject();
                    if (sortTuple.reverse()) {
                        builder.field("reverse", true);
                    }
                    builder.endObject();
                }
            }
            if (sortScripts != null) {
                for (ScriptSortTuple scriptSort : sortScripts) {
                    builder.startObject("_script");
                    builder.field("script", scriptSort.script());
                    builder.field("type", scriptSort.type());
                    if (scriptSort.params() != null) {
                        builder.field("params");
                        builder.map(scriptSort.params());
                    }
                    if (scriptSort.reverse()) {
                        builder.field("reverse", true);
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
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

        if (highlightBuilder != null) {
            highlightBuilder.toXContent(builder, params);
        }

        builder.endObject();
    }

    private static class ScriptField {
        private final String fieldName;
        private final String script;
        private final Map<String, Object> params;

        private ScriptField(String fieldName, String script, Map<String, Object> params) {
            this.fieldName = fieldName;
            this.script = script;
            this.params = params;
        }

        public String fieldName() {
            return fieldName;
        }

        public String script() {
            return script;
        }

        public Map<String, Object> params() {
            return params;
        }
    }

    private static class SortTuple {
        private final String fieldName;
        private final boolean reverse;

        private SortTuple(String fieldName, boolean reverse) {
            this.fieldName = fieldName;
            this.reverse = reverse;
        }

        public String fieldName() {
            return fieldName;
        }

        public boolean reverse() {
            return reverse;
        }
    }

    private static class ScriptSortTuple {
        private final String script;
        private final String type;
        private final Map<String, Object> params;
        private final boolean reverse;

        private ScriptSortTuple(String script, String type, Map<String, Object> params, boolean reverse) {
            this.script = script;
            this.type = type;
            this.params = params;
            this.reverse = reverse;
        }

        public String script() {
            return script;
        }

        public String type() {
            return type;
        }

        public Map<String, Object> params() {
            return params;
        }

        public boolean reverse() {
            return reverse;
        }
    }
}
