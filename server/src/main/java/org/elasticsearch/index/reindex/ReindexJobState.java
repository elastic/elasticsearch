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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public class ReindexJobState implements Task.Status, PersistentTaskState {

    // TODO: Name
    public static final String NAME = ReindexTask.NAME;

    public static final ConstructingObjectParser<ReindexJobState, Void> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ReindexJobState((String) a[0], (BulkByScrollResponse) a[1],
            (ElasticsearchException) a[2]));

    private static String EPHEMERAL_TASK_ID = "ephemeral_task_id";
    private static String REINDEX_RESPONSE = "reindex_response";
    private static String REINDEX_EXCEPTION = "reindex_exception";

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(),  new ParseField(EPHEMERAL_TASK_ID));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
            new ParseField(REINDEX_RESPONSE));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField(REINDEX_EXCEPTION));
    }

    private final TaskId ephemeralTaskId;
    private final BulkByScrollResponse reindexResponse;
    private final ElasticsearchException jobException;

    private ReindexJobState(String ephemeralTaskId, BulkByScrollResponse reindexResponse, ElasticsearchException jobException) {
        this(new TaskId(ephemeralTaskId), reindexResponse, jobException);
    }

    ReindexJobState(TaskId ephemeralTaskId, @Nullable BulkByScrollResponse reindexResponse,
                    @Nullable ElasticsearchException jobException) {
        this.ephemeralTaskId = ephemeralTaskId;
        assert (reindexResponse == null) || (jobException == null) : "Either response or exception must be null";
        this.reindexResponse = reindexResponse;
        this.jobException = jobException;
    }

    public ReindexJobState(StreamInput in) throws IOException {
        ephemeralTaskId = TaskId.readFromStream(in);
        reindexResponse = in.readOptionalWriteable((input) -> {
            BulkByScrollResponse response = new BulkByScrollResponse();
            response.readFrom(input);
            return response;
        });
        jobException = in.readException();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ephemeralTaskId.writeTo(out);
        out.writeOptionalWriteable(reindexResponse);
        out.writeException(jobException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(EPHEMERAL_TASK_ID, ephemeralTaskId.toString());
        if (reindexResponse != null) {
            builder.field(REINDEX_RESPONSE);
            builder.startObject();
            reindexResponse.toXContent(builder, params);
            builder.endObject();
        }
        if (jobException != null) {
            builder.field(REINDEX_EXCEPTION);
            builder.startObject();
            jobException.toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
    }

    public BulkByScrollResponse getReindexResponse() {
        return reindexResponse;
    }

    public ElasticsearchException getJobException() {
        return jobException;
    }

    public TaskId getEphemeralTaskId() {
        return ephemeralTaskId;
    }

    public static ReindexJobState fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
