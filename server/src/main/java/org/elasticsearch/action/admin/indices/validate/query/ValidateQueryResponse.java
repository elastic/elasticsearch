/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * The response of the validate action.
 *
 *
 */
public class ValidateQueryResponse extends BroadcastResponse {

    public static final String VALID_FIELD = "valid";
    public static final String EXPLANATIONS_FIELD = "explanations";

    private final boolean valid;

    private final List<QueryExplanation> queryExplanations;

    ValidateQueryResponse(
        boolean valid,
        List<QueryExplanation> queryExplanations,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.valid = valid;
        this.queryExplanations = queryExplanations == null ? Collections.emptyList() : queryExplanations;
    }

    /**
     * A boolean denoting whether the query is valid.
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * The list of query explanations.
     */
    public List<? extends QueryExplanation> getQueryExplanation() {
        return queryExplanations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(valid);
        out.writeCollection(queryExplanations);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(VALID_FIELD, isValid());
        if (getQueryExplanation() != null && getQueryExplanation().isEmpty() == false) {
            builder.startArray(EXPLANATIONS_FIELD);
            for (QueryExplanation explanation : getQueryExplanation()) {
                builder.startObject();
                explanation.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }
}
