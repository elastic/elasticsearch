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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
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
    static ConstructingObjectParser<ValidateQueryResponse, Void> PARSER = new ConstructingObjectParser<>(
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

    private boolean valid;

    private List<QueryExplanation> queryExplanations;

    ValidateQueryResponse(StreamInput in) throws IOException {
        super(in);
        valid = in.readBoolean();
        int size = in.readVInt();
        if (size > 0) {
            queryExplanations = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                queryExplanations.add(new QueryExplanation(in));
            }
        }
    }

    ValidateQueryResponse(boolean valid, List<QueryExplanation> queryExplanations, int totalShards, int successfulShards, int failedShards,
                          List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.valid = valid;
        this.queryExplanations = queryExplanations;
        if (queryExplanations == null) {
            this.queryExplanations = Collections.emptyList();
        }
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
        if (queryExplanations == null) {
            return Collections.emptyList();
        }
        return queryExplanations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(valid);
        out.writeVInt(queryExplanations.size());
        for (QueryExplanation exp : queryExplanations) {
            exp.writeTo(out);
        }

    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(VALID_FIELD, isValid());
        if (getQueryExplanation() != null && !getQueryExplanation().isEmpty()) {
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
