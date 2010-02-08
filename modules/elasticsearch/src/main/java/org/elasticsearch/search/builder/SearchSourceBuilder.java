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
import org.elasticsearch.util.json.JsonBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchSourceBuilder {

    public static SearchSourceBuilder searchSource() {
        return new SearchSourceBuilder();
    }

    public static SearchSourceFacetsBuilder facets() {
        return new SearchSourceFacetsBuilder();
    }

    private JsonQueryBuilder queryBuilder;

    private int from = -1;

    private int size = -1;

    private String queryParserName;

    private Boolean explain;

    private List<SortTuple> sortFields;

    private List<String> fieldNames;

    private SearchSourceFacetsBuilder facetsBuilder;

    public SearchSourceBuilder() {
    }

    public SearchSourceBuilder query(JsonQueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    public SearchSourceBuilder from(int from) {
        this.from = from;
        return this;
    }

    public SearchSourceBuilder size(int size) {
        this.size = size;
        return this;
    }

    public SearchSourceBuilder queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
        return this;
    }

    public SearchSourceBuilder explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public SearchSourceBuilder sort(String name, boolean reverse) {
        return sort(name, null, reverse);
    }

    public SearchSourceBuilder sort(String name) {
        return sort(name, null, false);
    }

    public SearchSourceBuilder sort(String name, String type) {
        return sort(name, type, false);
    }

    public SearchSourceBuilder sort(String name, String type, boolean reverse) {
        if (sortFields == null) {
            sortFields = newArrayListWithCapacity(2);
        }
        sortFields.add(new SortTuple(name, reverse, type));
        return this;
    }

    public SearchSourceBuilder facets(SearchSourceFacetsBuilder facetsBuilder) {
        this.facetsBuilder = facetsBuilder;
        return this;
    }

    public SearchSourceBuilder fields(List<String> fields) {
        this.fieldNames = fields;
        return this;
    }

    public SearchSourceBuilder field(String name) {
        if (fieldNames == null) {
            fieldNames = new ArrayList<String>();
        }
        fieldNames.add(name);
        return this;
    }

    public String build() {
        try {
            JsonBuilder builder = JsonBuilder.cached();
            builder.startObject();

            if (from != -1) {
                builder.field("from", from);
            }
            if (size != -1) {
                builder.field("size", size);
            }
            if (queryParserName != null) {
                builder.field("queryParserName", queryParserName);
            }

            builder.field("query");
            queryBuilder.toJson(builder);

            if (explain != null) {
                builder.field("explain", explain);
            }

            if (fieldNames != null) {
                if (fieldNames.size() == 1) {
                    builder.field("fields", fieldNames.get(0));
                } else {
                    builder.startArray("fields");
                    for (String fieldName : fieldNames) {
                        builder.string(fieldName);
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

            if (facetsBuilder != null) {
                facetsBuilder.json(builder);
            }

            builder.endObject();

            return builder.string();
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
