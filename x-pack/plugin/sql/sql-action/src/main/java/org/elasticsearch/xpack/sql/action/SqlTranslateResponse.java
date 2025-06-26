/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for the sql action for translating SQL queries into ES requests
 */
public class SqlTranslateResponse extends ActionResponse implements ToXContentObject {
    private final SearchSourceBuilder source;

    public SqlTranslateResponse(StreamInput in) throws IOException {
        source = new SearchSourceBuilder(in);
    }

    public SqlTranslateResponse(SearchSourceBuilder source) {
        this.source = source;
    }

    public SearchSourceBuilder source() {
        return source;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source.writeTo(out);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SqlTranslateResponse other = (SqlTranslateResponse) obj;
        return Objects.equals(source, other.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return source.toXContent(builder, params);
    }
}
