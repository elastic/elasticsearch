/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response of the validate action.
 *
 *
 */
public class ValidateQueryResponse extends BroadcastResponse {

    public static final String VALID_FIELD = "valid";
    public static final String EXPLANATIONS_FIELD = "explanations";

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ValidateQueryResponse, Void> PARSER = new ConstructingObjectParser<>(
        "validate_query",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return
                new ValidateQueryResponse(
                    (boolean)arg[1],
                    (List<QueryExplanation>)arg[2],
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getFailedShards(),
                    Arrays.asList(response.getShardFailures())
                );
        }
    );
    static {
        declareBroadcastFields(PARSER);
        PARSER.declareBoolean(constructorArg(), new ParseField(VALID_FIELD));
        PARSER.declareObjectArray(
            optionalConstructorArg(), QueryExplanation.PARSER, new ParseField(EXPLANATIONS_FIELD)
        );
    }

    private final boolean valid;

    private final List<QueryExplanation> queryExplanations;

    ValidateQueryResponse(StreamInput in) throws IOException {
        super(in);
        valid = in.readBoolean();
        queryExplanations = in.readList(QueryExplanation::new);
    }

    ValidateQueryResponse(boolean valid, List<QueryExplanation> queryExplanations, int totalShards, int successfulShards, int failedShards,
                          List<DefaultShardOperationFailedException> shardFailures) {
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

    public static ValidateQueryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
