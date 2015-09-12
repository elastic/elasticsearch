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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Represents an alias, to be associated with an index
 */
public class Alias implements Streamable {

    private String name;

    @Nullable
    private String filter;

    @Nullable
    private String indexRouting;

    @Nullable
    private String searchRouting;

    private Alias() {

    }

    public Alias(String name) {
        this.name = name;
    }

    /**
     * Returns the alias name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the filter associated with the alias
     */
    public String filter() {
        return filter;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(String filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(filter);
            this.filter = builder.string();
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(QueryBuilder filterBuilder) {
        if (filterBuilder == null) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.close();
            this.filter = builder.string();
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to build json for alias request", e);
        }
    }

    /**
     * Associates a routing value to the alias
     */
    public Alias routing(String routing) {
        this.indexRouting = routing;
        this.searchRouting = routing;
        return this;
    }

    /**
     * Returns the index routing value associated with the alias
     */
    public String indexRouting() {
        return indexRouting;
    }

    /**
     * Associates an index routing value to the alias
     */
    public Alias indexRouting(String indexRouting) {
        this.indexRouting = indexRouting;
        return this;
    }

    /**
     * Returns the search routing value associated with the alias
     */
    public String searchRouting() {
        return searchRouting;
    }

    /**
     * Associates a search routing value to the alias
     */
    public Alias searchRouting(String searchRouting) {
        this.searchRouting = searchRouting;
        return this;
    }

    /**
     * Allows to read an alias from the provided input stream
     */
    public static Alias read(StreamInput in) throws IOException {
        Alias alias = new Alias();
        alias.readFrom(in);
        return alias;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        filter = in.readOptionalString();
        indexRouting = in.readOptionalString();
        searchRouting = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(filter);
        out.writeOptionalString(indexRouting);
        out.writeOptionalString(searchRouting);
    }

    /**
     * Parses an alias and returns its parsed representation
     */
    public static Alias fromXContent(XContentParser parser) throws IOException {
        Alias alias = new Alias(parser.currentName());

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            throw new IllegalArgumentException("No alias is specified");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filter".equals(currentFieldName)) {
                    Map<String, Object> filter = parser.mapOrdered();
                    alias.filter(filter);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("routing".equals(currentFieldName)) {
                    alias.routing(parser.text());
                } else if ("index_routing".equals(currentFieldName) || "indexRouting".equals(currentFieldName) || "index-routing".equals(currentFieldName)) {
                    alias.indexRouting(parser.text());
                } else if ("search_routing".equals(currentFieldName) || "searchRouting".equals(currentFieldName) || "search-routing".equals(currentFieldName)) {
                    alias.searchRouting(parser.text());
                }
            }
        }
        return alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alias alias = (Alias) o;

        if (name != null ? !name.equals(alias.name) : alias.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
