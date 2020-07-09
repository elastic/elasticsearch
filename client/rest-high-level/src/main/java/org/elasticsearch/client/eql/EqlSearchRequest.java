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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class EqlSearchRequest implements Validatable, ToXContentObject {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);

    private QueryBuilder filter = null;
    private String timestampField = "@timestamp";
    private String eventCategoryField = "event.category";
    private boolean isCaseSensitive = true;

    private int size = 10;
    private int fetchSize = 1000;
    private String query;
    private String tiebreakerField;

    // Async settings
    private TimeValue waitForCompletionTimeout;
    private boolean keepOnCompletion;
    private TimeValue keepAlive;

    static final String KEY_FILTER = "filter";
    static final String KEY_TIMESTAMP_FIELD = "timestamp_field";
    static final String KEY_TIEBREAKER_FIELD = "tiebreaker_field";
    static final String KEY_EVENT_CATEGORY_FIELD = "event_category_field";
    static final String KEY_CASE_SENSITIVE = "case_sensitive";
    static final String KEY_SIZE = "size";
    static final String KEY_FETCH_SIZE = "fetch_size";
    static final String KEY_QUERY = "query";
    static final String KEY_WAIT_FOR_COMPLETION_TIMEOUT = "wait_for_completion_timeout";
    static final String KEY_KEEP_ALIVE = "keep_alive";
    static final String KEY_KEEP_ON_COMPLETION = "keep_on_completion";

    public EqlSearchRequest(String indices, String query) {
        indices(indices);
        query(query);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (filter != null) {
            builder.field(KEY_FILTER, filter);
        }
        builder.field(KEY_TIMESTAMP_FIELD, timestampField());
        if (tiebreakerField != null) {
            builder.field(KEY_TIEBREAKER_FIELD, tiebreakerField());
        }
        builder.field(KEY_EVENT_CATEGORY_FIELD, eventCategoryField());
        builder.field(KEY_SIZE, size());
        builder.field(KEY_FETCH_SIZE, fetchSize());
        builder.field(KEY_CASE_SENSITIVE, isCaseSensitive());

        builder.field(KEY_QUERY, query);
        if (waitForCompletionTimeout != null) {
            builder.field(KEY_WAIT_FOR_COMPLETION_TIMEOUT, waitForCompletionTimeout);
        }
        if (keepAlive != null) {
            builder.field(KEY_KEEP_ALIVE, keepAlive);
        }
        builder.field(KEY_KEEP_ON_COMPLETION, keepOnCompletion);
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

    public QueryBuilder filter() {
        return this.filter;
    }

    public EqlSearchRequest filter(QueryBuilder filter) {
        this.filter = filter;
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

    public String tiebreakerField() {
        return this.tiebreakerField;
    }

    public EqlSearchRequest tiebreakerField(String tiebreakerField) {
        Objects.requireNonNull(tiebreakerField, "tiebreaker field must not be null");
        this.tiebreakerField = tiebreakerField;
        return this;
    }

    public String eventCategoryField() {
        return this.eventCategoryField;
    }

    public EqlSearchRequest eventCategoryField(String eventCategoryField) {
        Objects.requireNonNull(eventCategoryField, "event category field must not be null");
        this.eventCategoryField = eventCategoryField;
        return this;
    }

    public boolean isCaseSensitive() {
        return this.isCaseSensitive;
    }

    public EqlSearchRequest isCaseSensitive(boolean isCaseSensitive) {
        this.isCaseSensitive = isCaseSensitive;
        return this;
    }

    public int size() {
        return this.size;
    }

    public EqlSearchRequest size(int size) {
        this.size = size;
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("size must be greater than 0");
        }
        return this;
    }

    public int fetchSize() {
        return this.fetchSize;
    }

    public EqlSearchRequest fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        if (fetchSize < 2) {
            throw new IllegalArgumentException("fetch size must be greater than 1");
        }
        return this;
    }

    public String query() {
        return this.query;
    }

    public EqlSearchRequest query(String query) {
        Objects.requireNonNull(query, "query must not be null");
        this.query = query;
        return this;
    }

    public TimeValue waitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public EqlSearchRequest waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        return this;
    }

    public Boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    public void keepOnCompletion(Boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    public EqlSearchRequest keepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
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
        return size == that.size &&
                fetchSize == that.fetchSize &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(indicesOptions, that.indicesOptions) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(timestampField, that.timestampField) &&
                Objects.equals(tiebreakerField, that.tiebreakerField) &&
                Objects.equals(eventCategoryField, that.eventCategoryField) &&
                Objects.equals(query, that.query) &&
                Objects.equals(isCaseSensitive, that.isCaseSensitive) &&
                Objects.equals(waitForCompletionTimeout, that.waitForCompletionTimeout) &&
                Objects.equals(keepAlive, that.keepAlive) &&
                Objects.equals(keepOnCompletion, that.keepOnCompletion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(indices),
            indicesOptions,
            filter,
            size,
            fetchSize,
            timestampField,
            tiebreakerField,
            eventCategoryField,
            query,
            isCaseSensitive,
            waitForCompletionTimeout,
            keepAlive,
            keepOnCompletion);
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
