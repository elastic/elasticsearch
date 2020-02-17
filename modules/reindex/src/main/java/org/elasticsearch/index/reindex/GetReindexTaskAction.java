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
package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class GetReindexTaskAction extends ActionType<GetReindexTaskAction.Response> {

    public static final GetReindexTaskAction INSTANCE = new GetReindexTaskAction();
    public static final String NAME = "indices:data/write/reindex/get";

    public GetReindexTaskAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String id;

        public Request(String id) {
            this.id = id;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        public String getId() {
            return id;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ConstructingObjectParser<Response, Void> PARSER =
            new ConstructingObjectParser<>("reindex/get_task", a -> new Response(
                (String) a[0],
                (String) a[1],
                (Long) a[2],
                (Long) a[3],
                (BulkByScrollResponse) a[4],
                (ElasticsearchException) a[5]));

        private static final String ID = "id";
        private static final String STATE = "state";
        private static final String START_TIME = "start_time";
        private static final String END_TIME = "end_time";
        private static final String DURATION = "duration";
        private static final String DURATION_IN_MILLIS = "duration_in_millis";
        private static final String REINDEX_RESULT = "result";
        private static final String REINDEX_FAILURE = "failure";

        private static final String RUNNING_STATE = "running";
        private static final String DONE_STATE = "done";
        private static final String FAILED_STATE = "failed";

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(ID));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(STATE));
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField(START_TIME)); // Fix millis
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(END_TIME)); // Fix millis
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
                new ParseField(REINDEX_RESULT));
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
                new ParseField(REINDEX_FAILURE));
        }

        private final String id;
        private final String state;
        private final long startMillis;
        private final Long endMillis;
        private final BulkByScrollResponse reindexResponse;
        private final ElasticsearchException exception;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.state = in.readString();
            this.startMillis = in.readLong();
            this.endMillis = in.readOptionalLong();
            this.reindexResponse = in.readOptionalWriteable(BulkByScrollResponse::new);
            this.exception = in.readOptionalWriteable(ElasticsearchException::new);
        }

        public Response(String id, long startMillis, @Nullable Long endMillis, @Nullable BulkByScrollResponse reindexResponse,
                        @Nullable ElasticsearchException exception) {
            this(id, getState(reindexResponse, exception), startMillis, endMillis, reindexResponse, exception);
        }

        public Response(String id, String state, long startMillis, @Nullable Long endMillis, @Nullable BulkByScrollResponse reindexResponse,
                        @Nullable ElasticsearchException exception) {
            this.id = id;
            this.state = state;
            this.startMillis = startMillis;
            this.endMillis = endMillis;
            this.reindexResponse = reindexResponse;
            this.exception = exception;
        }

        private static String getState(@Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception) {
            assert reindexResponse == null || exception == null;
            if (reindexResponse != null) {
                return DONE_STATE;
            } else if (exception != null) {
                return FAILED_STATE;
            } else {
                return RUNNING_STATE;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeString(state);
            out.writeLong(startMillis);
            out.writeOptionalLong(endMillis);
            out.writeOptionalWriteable(reindexResponse);
            out.writeOptionalWriteable(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            assert reindexResponse == null || exception == null;
            builder.field(STATE, state);
            builder.field(START_TIME, Instant.ofEpochMilli(startMillis).atZone(ZoneOffset.UTC));
            if (endMillis != null) {
                builder.field(END_TIME, Instant.ofEpochMilli(endMillis).atZone(ZoneOffset.UTC));
                builder.humanReadableField(DURATION_IN_MILLIS, DURATION, new TimeValue(endMillis - startMillis, TimeUnit.MILLISECONDS));
            }
            if (reindexResponse != null) {
                builder.field(REINDEX_RESULT);
                builder.startObject();
                reindexResponse.toXContent(builder, params);
                builder.endObject();
            } else if (exception != null) {
                builder.field(STATE, FAILED_STATE);
                builder.field(REINDEX_FAILURE);
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, exception);
                builder.endObject();
            }

            return builder.endObject();
        }

        public static Response fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }
}
