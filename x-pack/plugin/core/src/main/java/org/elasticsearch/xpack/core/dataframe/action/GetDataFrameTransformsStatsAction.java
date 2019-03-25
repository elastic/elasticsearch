/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetDataFrameTransformsStatsAction extends Action<GetDataFrameTransformsStatsAction.Response> {

    public static final GetDataFrameTransformsStatsAction INSTANCE = new GetDataFrameTransformsStatsAction();
    public static final String NAME = "cluster:monitor/data_frame/stats/get";
    public GetDataFrameTransformsStatsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContent {
        private String id;

        public Request(String id) {
            if (Strings.isNullOrEmpty(id) || id.equals("*")) {
                this.id = MetaData.ALL;
            } else {
                this.id = id;
            }
        }

        private Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
        }

        @Override
        public boolean match(Task task) {
            // If we are retrieving all the transforms, the task description does not contain the id
            if (id.equals(MetaData.ALL)) {
                return task.getDescription().startsWith(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX);
            }
            // Otherwise find the task by ID
            return task.getDescription().equals(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + id);
        }

        public String getId() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(DataFrameField.ID.getPreferredName(), id);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(id, other.id);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, GetDataFrameTransformsStatsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {
        private List<DataFrameTransformStateAndStats> transformsStateAndStats;

        public Response(List<DataFrameTransformStateAndStats> transformsStateAndStats) {
            super(Collections.emptyList(), Collections.emptyList());
            this.transformsStateAndStats = transformsStateAndStats;
        }

        public Response(List<DataFrameTransformStateAndStats> transformsStateAndStats, List<TaskOperationFailure> taskFailures,
                List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.transformsStateAndStats = transformsStateAndStats;
        }

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
            this.transformsStateAndStats = Collections.emptyList();
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            readFrom(in);
        }

        public List<DataFrameTransformStateAndStats> getTransformsStateAndStats() {
            return transformsStateAndStats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            transformsStateAndStats = in.readList(DataFrameTransformStateAndStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(transformsStateAndStats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field(DataFrameField.COUNT.getPreferredName(), transformsStateAndStats.size());
            builder.field(DataFrameField.TRANSFORMS.getPreferredName(), transformsStateAndStats);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformsStateAndStats);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final Response that = (Response) other;
            return Objects.equals(this.transformsStateAndStats, that.transformsStateAndStats);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
