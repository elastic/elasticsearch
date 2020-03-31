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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetReindexTaskAction extends ActionType<GetReindexTaskAction.Response> {

    public static final GetReindexTaskAction INSTANCE = new GetReindexTaskAction();
    public static final String NAME = "indices:data/write/reindex/get";

    public GetReindexTaskAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String[] ids;

        public Request(String[] ids) {
            this.ids = ids;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.ids = in.readStringArray();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(ids);
        }

        public String[] getId() {
            return ids;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "reindex/get_reindex_task",
            true,
            args -> {
                @SuppressWarnings("unchecked") // We're careful about the type in the list
                List<ReindexTaskResponse> jobs = (List<ReindexTaskResponse>) args[0];
                return new Response(unmodifiableList(jobs));
            });

        private static final String TASKS = "tasks";

        static {
            PARSER.declareObjectArray(constructorArg(), (p, c) -> ReindexTaskResponse.fromXContent(p), new ParseField(TASKS));
        }

        private List<ReindexTaskResponse> responses;

        public Response(StreamInput in) throws IOException {
            super(in);
            responses = in.readList(ReindexTaskResponse::new);
        }

        public Response(List<ReindexTaskResponse> responses) {
            this.responses = responses;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(responses);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TASKS);
            builder.startArray();
            for (ReindexTaskResponse response : responses) {
                response.toXContent(builder, params);
            }
            builder.endArray();
            return builder.endObject();
        }

        public static Response fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public static class ReindexTaskResponse implements ToXContentObject, Writeable {

        private static final ObjectParser<ReindexTaskWrapperBuilder, Void> PARSER =
            new ObjectParser<>(
                "reindex/get_task_wrapper",
                true,
                ReindexTaskWrapperBuilder::new
            );

        private static final String ID = "id";
        private static final String STATE = "state";
        private static final String START_TIME = "start_time";
        private static final String END_TIME = "end_time";
        private static final String DURATION = "duration";
        private static final String DURATION_IN_MILLIS = "duration_in_millis";
        private static final String FATAL_FAILURE = "fatal_failure";
        private static final String FAILURES = "failures";

        private static final String RUNNING_STATE = "running";
        private static final String DONE_STATE = "done";
        private static final String TIMED_OUT_STATE = "timed_out";
        private static final String FAILED_STATE = "failed";

        static {
            PARSER.declareString(ReindexTaskWrapperBuilder::setId, new ParseField(ID));
            PARSER.declareString(ReindexTaskWrapperBuilder::setState, new ParseField(STATE));
            PARSER.declareLong(ReindexTaskWrapperBuilder::setStartMillis, new ParseField(START_TIME));
            PARSER.declareLong(ReindexTaskWrapperBuilder::setEndMillis, new ParseField(END_TIME));
            PARSER.declareObject(ReindexTaskWrapperBuilder::setFatalFailure, (p, c) -> ElasticsearchException.fromXContent(p),
                new ParseField(FATAL_FAILURE));
            PARSER.declareObjectArray(
                ReindexTaskWrapperBuilder::setFailures, (p, c) -> BulkByScrollResponse.parseFailure(p), new ParseField(FAILURES)
            );
            // since the result of BulkByScrollResponse.Status are mixed we also parse that in this
            BulkByScrollTask.Status.declareFields(PARSER);
        }

        private final String id;
        private final String state;
        private final long startMillis;
        private final Long endMillis;
        private final BulkByScrollTask.Status status;
        private final List<BulkItemResponse.Failure> bulkFailures;
        private final List<ScrollableHitSource.SearchFailure> searchFailures;
        private final ElasticsearchException fatalFailure;

        public ReindexTaskResponse(StreamInput in) throws IOException {
            this.id = in.readString();
            this.state = in.readString();
            this.startMillis = in.readLong();
            this.endMillis = in.readOptionalLong();
            this.status = in.readOptionalWriteable(BulkByScrollTask.Status::new);
            this.bulkFailures = in.readList(BulkItemResponse.Failure::new);
            this.searchFailures = in.readList(ScrollableHitSource.SearchFailure::new);
            this.fatalFailure = in.readOptionalWriteable(ElasticsearchException::new);
        }

        public ReindexTaskResponse(String id, String state, long startMillis, @Nullable Long endMillis,
                                   @Nullable BulkByScrollTask.Status status, @Nullable List<BulkItemResponse.Failure> bulkFailures,
                                   @Nullable List<ScrollableHitSource.SearchFailure> searchFailures,
                                   @Nullable ElasticsearchException fatalFailure) {
            this.id = id;
            this.state = state;
            this.startMillis = startMillis;
            this.endMillis = endMillis;
            this.status = status;
            this.bulkFailures = bulkFailures;
            this.searchFailures = searchFailures;
            this.fatalFailure = fatalFailure;
        }

        public static ReindexTaskResponse fromResponse(String id, long startMillis, Long endMillis, BulkByScrollResponse response) {
            return new ReindexTaskResponse(id, getState(response, null), startMillis, endMillis, response.getStatus(),
                response.getBulkFailures(), response.getSearchFailures(), null);
        }

        public static ReindexTaskResponse fromException(String id, long startMillis, Long endMillis, ElasticsearchException exception) {
            return new ReindexTaskResponse(id, getState(null, exception), startMillis, endMillis, null,
                null, null, exception);
        }

        public static ReindexTaskResponse fromInProgress(String id, long startMillis) {
            return new ReindexTaskResponse(id, getState(null, null), startMillis, null, null,
                null, null, null);
        }

        private static String getState(@Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException fatalException) {
            assert reindexResponse == null || fatalException == null;
            if (reindexResponse != null) {
                if (reindexResponse.isTimedOut()) {
                    return TIMED_OUT_STATE;
                } else {
                    return DONE_STATE;
                }
            } else if (fatalException != null) {
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
            out.writeOptionalWriteable(status);
            out.writeOptionalWriteable(fatalFailure);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(STATE, state);
            builder.field(START_TIME, Instant.ofEpochMilli(startMillis).atZone(ZoneOffset.UTC));
            if (endMillis != null) {
                builder.field(END_TIME, Instant.ofEpochMilli(endMillis).atZone(ZoneOffset.UTC));
                builder.humanReadableField(DURATION_IN_MILLIS, DURATION, new TimeValue(endMillis - startMillis, TimeUnit.MILLISECONDS));
            } else {
                final long nowMillis = Math.max(Instant.now().toEpochMilli(), startMillis);
                builder.humanReadableField(DURATION_IN_MILLIS, DURATION, new TimeValue(nowMillis - startMillis, TimeUnit.MILLISECONDS));
            }
            if (status != null) {
                status.innerXContent(builder, params);
            }
            if (fatalFailure != null) {
                builder.field(FATAL_FAILURE);
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, fatalFailure);
                builder.endObject();
            }
            builder.startArray("failures");
            for (BulkItemResponse.Failure failure: bulkFailures) {
                builder.startObject();
                failure.toXContent(builder, params);
                builder.endObject();
            }
            for (ScrollableHitSource.SearchFailure failure: searchFailures) {
                failure.toXContent(builder, params);
            }
            builder.endArray();

            return builder.endObject();
        }

        public static ReindexTaskResponse fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null).buildWrapper();
        }
    }

    private static class ReindexTaskWrapperBuilder extends BulkByScrollTask.StatusBuilder {

        private String id;
        private String state;
        private long startMillis;
        private Long endMillis;
        private ElasticsearchException fatalFailure;
        private BulkByScrollTask.Status status;
        private List<BulkItemResponse.Failure> bulkFailures = new ArrayList<>();
        private List<ScrollableHitSource.SearchFailure> searchFailures = new ArrayList<>();

        ReindexTaskWrapperBuilder() {
        }


        public void setId(String id) {
            this.id = id;
        }

        public void setState(String state) {
            this.state = state;
        }

        public void setStartMillis(long startMillis) {
            this.startMillis = startMillis;
        }

        public void setEndMillis(Long endMillis) {
            this.endMillis = endMillis;
        }

        public void setFatalFailure(ElasticsearchException fatalFailure) {
            this.fatalFailure = fatalFailure;
        }

        public void setStatus(BulkByScrollTask.Status status) {
            this.status = status;
        }

        public void setFailures(List<Object> failures) {
            if (failures != null) {
                for (Object object : failures) {
                    if (object instanceof BulkItemResponse.Failure) {
                        bulkFailures.add((BulkItemResponse.Failure) object);
                    } else if (object instanceof ScrollableHitSource.SearchFailure) {
                        searchFailures.add((ScrollableHitSource.SearchFailure) object);
                    }
                }
            }
        }

        public ReindexTaskResponse buildWrapper() {
            status = super.buildStatus();
            return new ReindexTaskResponse(id, state, startMillis, endMillis, status, bulkFailures, searchFailures, fatalFailure);
        }
    }
}
