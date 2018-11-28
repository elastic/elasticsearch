/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.DataFrameJob;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.DataFrameJobConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetDataFrameJobsAction extends Action<GetDataFrameJobsAction.Response>{

    public static final GetDataFrameJobsAction INSTANCE = new GetDataFrameJobsAction();
    public static final String NAME = "cluster:monitor/data_frame/get";
    public static final ParseField JOBS = new ParseField("jobs");
    public static final ParseField COUNT = new ParseField("count");

    private GetDataFrameJobsAction() {
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

        public Request() {}

        @Override
        public boolean match(Task task) {
            // If we are retrieving all the jobs, the task description does not contain the id
            if (id.equals(MetaData.ALL)) {
                return task.getDescription().startsWith(DataFrameJob.PERSISTENT_TASK_DESCRIPTION_PREFIX);
            }
            // Otherwise find the task by ID
            return task.getDescription().equals(DataFrameJob.PERSISTENT_TASK_DESCRIPTION_PREFIX + id);
        }

        public String getId() {
            return id;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readString();
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
            builder.field(DataFrameJob.ID.getPreferredName(), id);
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

        protected RequestBuilder(ElasticsearchClient client, GetDataFrameJobsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private List<DataFrameJobConfig> jobConfigurations;

        public Response(List<DataFrameJobConfig> jobConfigs) {
            super(Collections.emptyList(), Collections.emptyList());
            this.jobConfigurations = jobConfigs;
        }

        public Response(List<DataFrameJobConfig> jobResponses, List<TaskOperationFailure> taskFailures,
                List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.jobConfigurations = jobResponses;
        }

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
        }

        public Response(StreamInput in) throws IOException {
            super(Collections.emptyList(), Collections.emptyList());
            readFrom(in);
        }

        public List<DataFrameJobConfig> getJobConfigurations() {
            return jobConfigurations;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobConfigurations = in.readList(DataFrameJobConfig::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(jobConfigurations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(COUNT.getPreferredName(), jobConfigurations.size());
            // XContentBuilder does not support passing the params object for Iterables
            builder.field(JOBS.getPreferredName());
            builder.startArray();
            for (DataFrameJobConfig jobResponse : jobConfigurations) {
                jobResponse.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobConfigurations);
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
            return Objects.equals(this.jobConfigurations, that.jobConfigurations);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
