/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;


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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStats;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetRollupJobsAction extends Action<GetRollupJobsAction.Response> {

    public static final GetRollupJobsAction INSTANCE = new GetRollupJobsAction();
    public static final String NAME = "cluster:monitor/xpack/rollup/get";
    public static final ParseField JOBS = new ParseField("jobs");
    public static final ParseField CONFIG = new ParseField("config");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField STATS = new ParseField("stats");

    private GetRollupJobsAction() {
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
            // If we are retrieving all the jobs, the task description just needs to start
            // with `rollup_`
            if (id.equals(MetaData.ALL)) {
                return task.getDescription().startsWith(RollupField.NAME + "_");
            }
            // Otherwise find the task by ID
            return task.getDescription().equals(RollupField.NAME + "_" + id);
        }

        public String getId() {
            return id;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readString();
            if (Strings.isNullOrEmpty(id) || id.equals("*")) {
                this.id = MetaData.ALL;
            }
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
            builder.field(RollupField.ID.getPreferredName(), id);
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

        protected RequestBuilder(ElasticsearchClient client, GetRollupJobsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private List<JobWrapper> jobs;

        public Response(List<JobWrapper> jobs) {
            super(Collections.emptyList(), Collections.emptyList());
            this.jobs = jobs;
        }

        public Response(List<JobWrapper> jobs, List<TaskOperationFailure> taskFailures, List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.jobs = jobs;
        }

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
        }

        public Response(StreamInput in) throws IOException {
            super(Collections.emptyList(), Collections.emptyList());
            readFrom(in);
        }

        public List<JobWrapper> getJobs() {
            return jobs;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobs = in.readList(JobWrapper::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(jobs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(JOBS.getPreferredName(), jobs);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobs);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(jobs, other.jobs);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class JobWrapper implements Writeable, ToXContentObject {
        private final RollupJobConfig job;
        private final RollupJobStats stats;
        private final RollupJobStatus status;

        public static final ConstructingObjectParser<JobWrapper, Void> PARSER
                = new ConstructingObjectParser<>(NAME, a -> new JobWrapper((RollupJobConfig) a[0],
                (RollupJobStats) a[1], (RollupJobStatus)a[2]));

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> RollupJobConfig.PARSER.apply(p,c).build(), CONFIG);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), RollupJobStats.PARSER::apply, STATS);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), RollupJobStatus.PARSER::apply, STATUS);
        }

        public JobWrapper(RollupJobConfig job, RollupJobStats stats, RollupJobStatus status) {
            this.job = job;
            this.stats = stats;
            this.status = status;
        }

        public JobWrapper(StreamInput in) throws IOException {
            this.job = new RollupJobConfig(in);
            this.stats = new RollupJobStats(in);
            this.status = new RollupJobStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            job.writeTo(out);
            stats.writeTo(out);
            status.writeTo(out);
        }

        public RollupJobConfig getJob() {
            return job;
        }

        public RollupJobStats getStats() {
            return stats;
        }

        public RollupJobStatus getStatus() {
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONFIG.getPreferredName());
            job.toXContent(builder, params);
            builder.field(STATUS.getPreferredName(), status);
            builder.field(STATS.getPreferredName(), stats);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(job, stats, status);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            JobWrapper other = (JobWrapper) obj;
            return Objects.equals(job, other.job)
                    && Objects.equals(stats, other.stats)
                    && Objects.equals(status, other.status);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
