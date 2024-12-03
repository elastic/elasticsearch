/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class MockDefaultInferenceQueryBuilder extends AbstractQueryBuilder<MockDefaultInferenceQueryBuilder> {

    public static final String NAME = "mock_default_inference";

    private final String fieldName;
    private final String query;
    private final Boolean throwOnUnsupportedFields;

    public MockDefaultInferenceQueryBuilder(String fieldName, String query, Boolean throwOnUnsupportedFields) {
        this.fieldName = fieldName;
        this.query = query;
        this.throwOnUnsupportedFields = throwOnUnsupportedFields;
    }

    public MockDefaultInferenceQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        query = in.readString();
        throwOnUnsupportedFields = in.readOptionalBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(query);
        out.writeOptionalBoolean(throwOnUnsupportedFields);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    protected boolean doEquals(MockDefaultInferenceQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(throwOnUnsupportedFields, other.throwOnUnsupportedFields);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, throwOnUnsupportedFields);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }
}
