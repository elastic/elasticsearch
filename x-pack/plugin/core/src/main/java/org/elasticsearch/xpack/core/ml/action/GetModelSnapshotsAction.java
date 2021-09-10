/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetModelSnapshotsAction extends ActionType<GetModelSnapshotsAction.Response> {

    public static final GetModelSnapshotsAction INSTANCE = new GetModelSnapshotsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/model_snapshots/get";

    private GetModelSnapshotsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
        public static final ParseField SORT = new ParseField("sort");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField DESC = new ParseField("desc");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString((request, snapshotId) -> request.snapshotId = snapshotId, SNAPSHOT_ID);
            PARSER.declareString(Request::setStart, START);
            PARSER.declareString(Request::setEnd, END);
            PARSER.declareString(Request::setSort, SORT);
            PARSER.declareBoolean(Request::setDescOrder, DESC);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
        }

        public static Request parseRequest(String jobId, String snapshotId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            if (snapshotId != null) {
                request.snapshotId = snapshotId;
            }
            return request;
        }

        private String jobId;
        private String snapshotId;
        private String sort;
        private String start;
        private String end;
        private boolean desc = true;
        private PageParams pageParams = new PageParams();

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            snapshotId = in.readOptionalString();
            sort = in.readOptionalString();
            start = in.readOptionalString();
            end = in.readOptionalString();
            desc = in.readBoolean();
            pageParams = new PageParams(in);
        }

        public Request(String jobId, String snapshotId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.snapshotId = snapshotId;
        }

        public String getJobId() {
            return jobId;
        }

        @Nullable
        public String getSnapshotId() {
            return snapshotId;
        }

        @Nullable
        public String getSort() {
            return sort;
        }

        public void setSort(String sort) {
            this.sort = sort;
        }

        public boolean getDescOrder() {
            return desc;
        }

        public void setDescOrder(boolean desc) {
            this.desc = desc;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = ExceptionsHelper.requireNonNull(pageParams, PageParams.PAGE.getPreferredName());
        }

        @Nullable
        public String getStart() {
            return start;
        }

        public void setStart(String start) {
            this.start = ExceptionsHelper.requireNonNull(start, START.getPreferredName());
        }

        @Nullable
        public String getEnd() {
            return end;
        }

        public void setEnd(String end) {
            this.end = ExceptionsHelper.requireNonNull(end, END.getPreferredName());
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeOptionalString(snapshotId);
            out.writeOptionalString(sort);
            out.writeOptionalString(start);
            out.writeOptionalString(end);
            out.writeBoolean(desc);
            pageParams.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            if (snapshotId != null) {
                builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
            }
            if (start != null) {
                builder.field(START.getPreferredName(), start);
            }
            if (end != null) {
                builder.field(END.getPreferredName(), end);
            }
            if (sort != null) {
                builder.field(SORT.getPreferredName(), sort);
            }
            builder.field(DESC.getPreferredName(), desc);
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, snapshotId, start, end, sort, desc);
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
            return Objects.equals(jobId, other.jobId)
                    && Objects.equals(snapshotId, other.snapshotId)
                    && Objects.equals(start, other.start)
                    && Objects.equals(end, other.end)
                    && Objects.equals(sort, other.sort)
                    && Objects.equals(desc, other.desc);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<ModelSnapshot> implements ToXContentObject {

        public Response(QueryPage<ModelSnapshot> page) {
            super(page);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<ModelSnapshot> getPage() {
            return getResources();
        }

        @Override
        protected Reader<ModelSnapshot> getReader() {
            return ModelSnapshot::new;
        }
    }
}
