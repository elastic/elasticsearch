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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class StartReindexTaskAction extends ActionType<StartReindexTaskAction.Response> {

    public static final StartReindexTaskAction INSTANCE = new StartReindexTaskAction();
    public static final String NAME = "indices:data/write/reindex/start";

    private StartReindexTaskAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject, CompositeIndicesRequest {

        private final ReindexRequest reindexRequest;
        private final boolean waitForCompletion;
        private final boolean resilient;

        public Request(ReindexRequest reindexRequest, boolean waitForCompletion) {
            this(reindexRequest, waitForCompletion, true);
        }

        public Request(ReindexRequest reindexRequest, boolean waitForCompletion, boolean resilient) {
            this.reindexRequest = reindexRequest;
            this.waitForCompletion = waitForCompletion;
            this.resilient = resilient;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            reindexRequest = new ReindexRequest(in);
            waitForCompletion = in.readBoolean();
            resilient = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            reindexRequest.writeTo(out);
            out.writeBoolean(waitForCompletion);
            out.writeBoolean(resilient);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        public ReindexRequest getReindexRequest() {
            return reindexRequest;
        }

        public boolean getWaitForCompletion() {
            return waitForCompletion;
        }

        public boolean isResilient() {
            return resilient;
        }
    }

    public static class Response extends ActionResponse {

        static final ParseField EPHEMERAL_TASK_ID = new ParseField("ephemeral_task_id");
        static final ParseField PERSISTENT_TASK_ID = new ParseField("persistent_task_id");
        static final ParseField REINDEX_RESPONSE = new ParseField("reindex_response");

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "start_reindex_response", true, args -> new Response((String) args[1], (String) args[0], (BulkByScrollResponse) args[2]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), EPHEMERAL_TASK_ID);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), PERSISTENT_TASK_ID);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (parser, context) -> BulkByScrollResponse.fromXContent(parser), REINDEX_RESPONSE);
        }

        private final String persistentTaskId;
        private final String ephemeralTaskId;
        @Nullable private final BulkByScrollResponse reindexResponse;

        public Response(String persistentTaskId, String ephemeralTaskId) {
            this(persistentTaskId, ephemeralTaskId, null);
        }

        public Response(String persistentTaskId, String ephemeralTaskId, BulkByScrollResponse reindexResponse) {
            this.ephemeralTaskId = ephemeralTaskId;
            this.persistentTaskId = persistentTaskId;
            this.reindexResponse = reindexResponse;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            ephemeralTaskId = in.readString();
            persistentTaskId = in.readString();
            reindexResponse = in.readOptionalWriteable(BulkByScrollResponse::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(ephemeralTaskId);
            out.writeString(persistentTaskId);
            out.writeOptionalWriteable(reindexResponse);
        }

        public String getEphemeralTaskId() {
            return ephemeralTaskId;
        }

        public BulkByScrollResponse getReindexResponse() {
            return reindexResponse;
        }

        public String getPersistentTaskId() {
            return persistentTaskId;
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
