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

package org.elasticsearch.client.eql;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class EqlSearchRequest implements Validatable, ToXContentObject {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false,
        false, true, false);

    private QueryBuilder query = null;
    private String timestampField = "@timestamp";
    private String eventTypeField = "event_type";
    private String implicitJoinKeyField = "agent.id";
    private int fetchSize = 50;
    private SearchAfterBuilder searchAfterBuilder;
    private String rule;

    static final String KEY_QUERY = "query";
    static final String KEY_TIMESTAMP_FIELD = "timestamp_field";
    static final String KEY_EVENT_TYPE_FIELD = "event_type_field";
    static final String KEY_IMPLICIT_JOIN_KEY_FIELD = "implicit_join_key_field";
    static final String KEY_SIZE = "size";
    static final String KEY_SEARCH_AFTER = "search_after";
    static final String KEY_RULE = "rule";

    public EqlSearchRequest(String indices, String rule) {
        indices(indices);
        rule(rule);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (query != null) {
            builder.field(KEY_QUERY, query);
        }
        builder.field(KEY_TIMESTAMP_FIELD, timestampField());
        builder.field(KEY_EVENT_TYPE_FIELD, eventTypeField());
        if (implicitJoinKeyField != null) {
            builder.field(KEY_IMPLICIT_JOIN_KEY_FIELD, implicitJoinKeyField());
        }
        builder.field(KEY_SIZE, fetchSize());

        if (searchAfterBuilder != null) {
            builder.array(KEY_SEARCH_AFTER, searchAfterBuilder.getSortValues());
        }

        builder.field(KEY_RULE, rule);
        builder.endObject();
        return builder;
    }

    public EqlSearchRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
        this.indices = indices;
        return this;
    }

    public QueryBuilder query() {
        return this.query;
    }

    public EqlSearchRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    public String timestampField() {
        return this.timestampField;
    }

    public EqlSearchRequest timestampField(String timestampField) {
        Objects.requireNonNull(timestampField, "timestamp field must not be null");
        this.timestampField = timestampField;
        return this;
    }

    public String eventTypeField() {
        return this.eventTypeField;
    }

    public EqlSearchRequest eventTypeField(String eventTypeField) {
        Objects.requireNonNull(eventTypeField, "event type field must not be null");
        this.eventTypeField = eventTypeField;
        return this;
    }

    public String implicitJoinKeyField() {
        return this.implicitJoinKeyField;
    }

    public EqlSearchRequest implicitJoinKeyField(String implicitJoinKeyField) {
        Objects.requireNonNull(implicitJoinKeyField, "implicit join key must not be null");
        this.implicitJoinKeyField = implicitJoinKeyField;
        return this;
    }

    public int fetchSize() {
        return this.fetchSize;
    }

    public EqlSearchRequest fetchSize(int size) {
        this.fetchSize = size;
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("size must be more than 0");
        }
        return this;
    }

    public Object[] searchAfter() {
        if (searchAfterBuilder == null) {
            return null;
        }
        return searchAfterBuilder.getSortValues();
    }

    public EqlSearchRequest searchAfter(Object[] values) {
        this.searchAfterBuilder = new SearchAfterBuilder().setSortValues(values);
        return this;
    }

    private EqlSearchRequest setSearchAfter(SearchAfterBuilder builder) {
        this.searchAfterBuilder = builder;
        return this;
    }

    public String rule() {
        return this.rule;
    }

    public EqlSearchRequest rule(String rule) {
        Objects.requireNonNull(rule, "rule must not be null");
        this.rule = rule;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EqlSearchRequest that = (EqlSearchRequest) o;
        return
            fetchSize == that.fetchSize &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(indicesOptions, that.indicesOptions) &&
                Objects.equals(query, that.query) &&
                Objects.equals(timestampField, that.timestampField) &&
                Objects.equals(eventTypeField, that.eventTypeField) &&
                Objects.equals(implicitJoinKeyField, that.implicitJoinKeyField) &&
                Objects.equals(searchAfterBuilder, that.searchAfterBuilder) &&
                Objects.equals(rule, that.rule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(indices),
            indicesOptions,
            query,
            fetchSize,
            timestampField,
            eventTypeField,
            implicitJoinKeyField,
            searchAfterBuilder,
            rule);
    }

    public String[] indices() {
        return Arrays.copyOf(this.indices, this.indices.length);
    }

    public EqlSearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        return this;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }
}
