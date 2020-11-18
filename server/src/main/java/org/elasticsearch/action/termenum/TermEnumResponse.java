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

package org.elasticsearch.action.termenum;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
    public static final String TIMED_OUT_FIELD = "timed_out";

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<TermEnumResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_enum_results",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return new TermEnumResponse(
                (List<TermCount>) arg[1],
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
        PARSER.declareObjectArray(optionalConstructorArg(), TermCount.PARSER, new ParseField(TERMS_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(TIMED_OUT_FIELD));
    }

    private final List<TermCount> terms;

    private boolean timedOut;

    TermEnumResponse(StreamInput in) throws IOException {
        super(in);
        terms = in.readList(TermCount::new);
        timedOut = in.readBoolean();
    }

    TermEnumResponse(
        List<TermCount> terms,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures, boolean timedOut
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.terms = terms == null ? Collections.emptyList() : terms;
        this.timedOut = timedOut;
    }

    /**
     * The list of terms.
     */
    public List<TermCount> getTerms() {
        return terms;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(terms);
        out.writeBoolean(timedOut);

    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        if (getTerms() != null && !getTerms().isEmpty()) {
            builder.startArray(TERMS_FIELD);
            for (TermCount term : getTerms()) {
                builder.startObject();
                term.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.field("timed_out", timedOut);
    }

    public static TermEnumResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
