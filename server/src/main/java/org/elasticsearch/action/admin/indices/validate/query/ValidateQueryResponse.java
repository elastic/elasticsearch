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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.admin.indices.validate.query.QueryExplanation.readQueryExplanation;

/**
 * The response of the validate action.
 *
 *
 */
public class ValidateQueryResponse extends BroadcastResponse {

    public static final String INDEX_FIELD = "index";
    public static final String SHARD_FIELD = "shard";
    public static final String VALID_FIELD = "valid";
    public static final String EXPLANATIONS_FIELD = "explanations";
    public static final String ERROR_FIELD = "error";
    public static final String EXPLANATION_FIELD = "explanation";

    private boolean valid;

    private List<QueryExplanation> queryExplanations;

    ValidateQueryResponse() {

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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        valid = in.readBoolean();
        int size = in.readVInt();
        if (size > 0) {
            queryExplanations = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                queryExplanations.add(readQueryExplanation(in));
            }
        }
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
                if (explanation.getIndex() != null) {
                    builder.field(INDEX_FIELD, explanation.getIndex());
                }
                if(explanation.getShard() >= 0) {
                    builder.field(SHARD_FIELD, explanation.getShard());
                }
                builder.field(VALID_FIELD, explanation.isValid());
                if (explanation.getError() != null) {
                    builder.field(ERROR_FIELD, explanation.getError());
                }
                if (explanation.getExplanation() != null) {
                    builder.field(EXPLANATION_FIELD, explanation.getExplanation());
                }
                builder.endObject();
            }
            builder.endArray();
        }
    }
}
