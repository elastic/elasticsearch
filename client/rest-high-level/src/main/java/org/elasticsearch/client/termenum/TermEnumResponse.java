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
package org.elasticsearch.client.termenum;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response of the termenum/list action.
 */
public class TermEnumResponse extends BroadcastResponse {

    public static final String TERMS_FIELD = "terms";
    public static final String COMPLETE_FIELD = "complete";

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<TermEnumResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_enum_results",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return new TermEnumResponse(
                (List<String>) arg[1],
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures()),
                (Boolean) arg[2]
            );
        }
    );
    static {
        declareBroadcastFields(PARSER);
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(TERMS_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(COMPLETE_FIELD));
    }

    private final List<String> terms;

    private boolean complete;
    private int skippedShards;

    public TermEnumResponse(
        List<String> terms,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures, boolean complete
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.terms = terms == null ? Collections.emptyList() : terms;
        this.complete = complete;
    }

    /**
     * The list of terms.
     */
    public List<String> getTerms() {
        return terms;
    }
    
    /**
     * The number of shards skipped by the index filter
     */
    public int getSkippedShards() {
        return skippedShards;
    }    

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        if (getTerms() != null && getTerms().isEmpty() == false) {
            builder.startArray(TERMS_FIELD);
            for (String term : getTerms()) {
                builder.value(term);
            }
            builder.endArray();
        }
        builder.field(COMPLETE_FIELD, complete);
    }

    public static TermEnumResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
    
}
