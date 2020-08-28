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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;

/**
 * A response of a bulk execution. Holding a response for each item responding (in order) of the
 * bulk requests. Each item holds the index/type/id is operated on, and if it failed or not (with the
 * failure message).
 */
public class BulkResponse extends ActionResponse implements Iterable<BulkItemResponse>, StatusToXContentObject {

    private static final String ITEMS = "items";
    private static final String ERRORS = "errors";
    private static final String TOOK = "took";
    private static final String INGEST_TOOK = "ingest_took";

    public static final long NO_INGEST_TOOK = -1L;

    private final BulkItemResponse[] responses;
    private final long tookInMillis;
    private final long ingestTookInMillis;

    public BulkResponse(StreamInput in) throws IOException {
        super(in);
        responses = in.readArray(BulkItemResponse::new, BulkItemResponse[]::new);
        tookInMillis = in.readVLong();
        ingestTookInMillis = in.readZLong();
    }

    public BulkResponse(BulkItemResponse[] responses, long tookInMillis) {
        this(responses, tookInMillis, NO_INGEST_TOOK);
    }

    public BulkResponse(BulkItemResponse[] responses, long tookInMillis, long ingestTookInMillis) {
        this.responses = responses;
        this.tookInMillis = tookInMillis;
        this.ingestTookInMillis = ingestTookInMillis;
    }

    /**
     * How long the bulk execution took. Excluding ingest preprocessing.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time, otherwise 0 is returned.
     */
    public TimeValue getIngestTook() {
        return new TimeValue(ingestTookInMillis);
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time. in milliseconds, otherwise -1 is returned.
     */
    public long getIngestTookInMillis() {
        return ingestTookInMillis;
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        for (BulkItemResponse response : responses) {
            if (response.isFailed()) {
                return true;
            }
        }
        return false;
    }

    public String buildFailureMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (int i = 0; i < responses.length; i++) {
            BulkItemResponse response = responses[i];
            if (response.isFailed()) {
                sb.append("\n[").append(i)
                        .append("]: index [").append(response.getIndex())
                        .append("], id [").append(response.getId())
                        .append("], message [").append(response.getFailureMessage()).append("]");
            }
        }
        return sb.toString();
    }

    /**
     * The items representing each action performed in the bulk operation (in the same order!).
     */
    public BulkItemResponse[] getItems() {
        return responses;
    }

    @Override
    public Iterator<BulkItemResponse> iterator() {
        return Arrays.stream(responses).iterator();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(responses);
        out.writeVLong(tookInMillis);
        out.writeZLong(ingestTookInMillis);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOOK, tookInMillis);
        if (ingestTookInMillis != BulkResponse.NO_INGEST_TOOK) {
            builder.field(INGEST_TOOK, ingestTookInMillis);
        }
        builder.field(ERRORS, hasFailures());
        builder.startArray(ITEMS);
        for (BulkItemResponse item : this) {
            item.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static BulkResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

        long took = -1L;
        long ingestTook = NO_INGEST_TOOK;
        List<BulkItemResponse> items = new ArrayList<>();

        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TOOK.equals(currentFieldName)) {
                    took = parser.longValue();
                } else if (INGEST_TOOK.equals(currentFieldName)) {
                    ingestTook = parser.longValue();
                } else if (ERRORS.equals(currentFieldName) == false) {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (ITEMS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        items.add(BulkItemResponse.fromXContent(parser, items.size()));
                    }
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                throwUnknownToken(token, parser.getTokenLocation());
            }
        }
        return new BulkResponse(items.toArray(new BulkItemResponse[items.size()]), took, ingestTook);
    }
}
