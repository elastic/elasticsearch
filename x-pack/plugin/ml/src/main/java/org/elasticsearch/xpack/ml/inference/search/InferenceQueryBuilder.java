/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

public class InferenceQueryBuilder extends AbstractQueryBuilder<InferenceQueryBuilder>  {

    public static final String NAME = "ml_magic";

    private final String modelId;

    public static InferenceQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return null;
    }

    public InferenceQueryBuilder(String modelId) {
        this.modelId = modelId;
    }

    public InferenceQueryBuilder(StreamInput in) throws IOException {
        modelId = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return Queries.newMatchAllQuery();
    }

    @Override
    protected boolean doEquals(InferenceQueryBuilder other) {
        return Objects.equals(this.modelId, other.modelId);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(modelId);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
