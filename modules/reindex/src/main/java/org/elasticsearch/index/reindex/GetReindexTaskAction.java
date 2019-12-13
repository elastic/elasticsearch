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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class GetReindexTaskAction extends ActionType<GetReindexTaskAction.Response> {

    public static final GetReindexTaskAction INSTANCE = new GetReindexTaskAction();
    public static final String NAME = "indices:data/write/reindex/get";

    public GetReindexTaskAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String taskId;

        public Request(String taskId) {
            this.taskId = taskId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskId = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
        }

        public String getTaskId() {
            return taskId;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ConstructingObjectParser<Response, Void> PARSER =
            new ConstructingObjectParser<>("reindex/get_task", a -> new Response((BulkByScrollResponse) a[0],
                (ElasticsearchException) a[1]);

        private static final String REINDEX_RESPONSE = "response";
        private static final String REINDEX_EXCEPTION = "exception";

        static {
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
                new ParseField(REINDEX_RESPONSE));
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
                new ParseField(REINDEX_EXCEPTION));
        }

        private final BulkByScrollResponse reindexResponse;
        private final ElasticsearchException exception;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.reindexResponse = in.readOptionalWriteable(BulkByScrollResponse::new);
            this.exception = in.readOptionalWriteable(ElasticsearchException::new);
        }

        public Response(@Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception) {
            this.reindexResponse = reindexResponse;
            this.exception = exception;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(reindexResponse);
            out.writeOptionalWriteable(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (reindexResponse != null) {
                builder.field(REINDEX_RESPONSE);
                builder.startObject();
                reindexResponse.toXContent(builder, params);
                builder.endObject();
            }
            if (exception != null) {
                builder.field(REINDEX_EXCEPTION);
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
