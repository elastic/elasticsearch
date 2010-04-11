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

import org.elasticsearch.index.query.json.JsonQueryBuilder;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.util.gnu.trove.TObjectFloatHashMap;
import org.elasticsearch.util.gnu.trove.TObjectFloatIterator;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.json.ToJson;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.util.json.JsonBuilder.*;

/**
 * A search source builder allowing to easily build search source. Simple construction
 * using {@link org.elasticsearch.search.builder.SearchSourceBuilder#searchSource()}.
 *
 * @author kimchy (shay.banon)
 * @see org.elasticsearch.action.search.SearchRequest#source(SearchSourceBuilder)
 */
public class SearchSourceBuilder {

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
     * A static factory method to construct new search facets.
     */
    public static SearchSourceFacetsBuilder facets() {
        return new SearchSourceFacetsBuilder();
    }

    /**
     * A static factory method to construct new search highlights.
     */
    public static SearchSourceHighlightBuilder highlight() {
        return new SearchSourceHighlightBuilder();
    }

    private JsonQueryBuilder queryBuilder;

    private int from = -1;

    private int size = -1;

    private String queryParserName;

    private Boolean explain;

    private List<SortTuple> sortFields;

    private List<String> fieldNames;

    private SearchSourceFacetsBuilder facetsBuilder;

    private SearchSourceHighlightBuilder highlightBuilder;

    private TObjectFloatHashMap<String> indexBoost = null;


    /**
     * Constructs a new search source builder.
     */
    public SearchSourceBuilder() {
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.json.JsonQueryBuilders
     */
    public SearchSourceBuilder query(JsonQueryBuilder query) {
        this.queryBuilder = query;
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
        if (name.equals("score")) {
            if (order == Order.ASC) {
                reverse = true;
            }
        } else {
            if (order == Order.DESC) {
                reverse = true;
            }
        }
        return sort(name, null, reverse);
    }

    /**
     * Add a sort against the given field name and if it should be revered or not.
     *
     * @param name    The name of the field to sort by
     * @param reverse Should be soring be reversed or not
     */
    public SearchSourceBuilder sort(String name, boolean reverse) {
        return sort(name, null, reverse);
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name The name of the field to sort by
     */
    public SearchSourceBuilder sort(String name) {
        return sort(name, null, false);
    }

    /**
     * Add a sort against the given field name of the given type.
     *
     * @param name The name of the field to sort by
     * @param type The type of sort to perform
     */
    public SearchSourceBuilder sort(String name, String type) {
        return sort(name, type, false);
    }

    /**
     * Add a sort against the given field name and if it should be revered or not.
     *
     * @param name    The name of the field to sort by
     * @param type    The type of the sort to perform
     * @param reverse Should the sort be reversed or not
     */
    public SearchSourceBuilder sort(String name, String type, boolean reverse) {
        if (sortFields == null) {
            sortFields = newArrayListWithCapacity(2);
        }
        sortFields.add(new SortTuple(name, reverse, type));
        return this;
    }

    /**
     * Adds facets to perform as part of the search.
     */
    public SearchSourceBuilder facets(SearchSourceFacetsBuilder facetsBuilder) {
        this.facetsBuilder = facetsBuilder;
        return this;
    }

    /**
     * Adds highlight to perform as part of the search.
     */
    public SearchSourceBuilder highlight(SearchSourceHighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Sets the fields to load and return as part of the search request. If none are specified,
     * the source of the document will be returend.
     */
    public SearchSourceBuilder fields(List<String> fields) {
        this.fieldNames = fields;
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


    public byte[] build() throws SearchException {
        try {
            JsonBuilder builder = binaryJsonBuilder();
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
                queryBuilder.toJson(builder, ToJson.EMPTY_PARAMS);
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

            if (sortFields != null) {
                builder.field("sort");
                builder.startObject();
                for (SortTuple sortTuple : sortFields) {
                    builder.field(sortTuple.fieldName());
                    builder.startObject();
                    if (sortTuple.reverse) {
                        builder.field("reverse", true);
                    }
                    if (sortTuple.type != null) {
                        builder.field("type", sortTuple.type());
                    }
                    builder.endObject();
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

            if (facetsBuilder != null) {
                facetsBuilder.toJson(builder, ToJson.EMPTY_PARAMS);
            }

            if (highlightBuilder != null) {
                highlightBuilder.toJson(builder, ToJson.EMPTY_PARAMS);
            }

            builder.endObject();

            return builder.copiedBytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }

    private static class SortTuple {
        private final String fieldName;
        private final boolean reverse;
        private final String type;

        private SortTuple(String fieldName, boolean reverse, String type) {
            this.fieldName = fieldName;
            this.reverse = reverse;
            this.type = type;
        }

        public String fieldName() {
            return fieldName;
        }

        public boolean reverse() {
            return reverse;
        }

        public String type() {
            return type;
        }
    }
}
