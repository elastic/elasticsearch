/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;

import java.io.IOException;
import java.util.Objects;

public class PostDataAction extends ActionType<PostDataAction.Response> {

    public static final PostDataAction INSTANCE = new PostDataAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/data/post";

    private PostDataAction() {
        super(NAME, PostDataAction.Response::new);
    }

    public static class Response extends BaseTasksResponse implements StatusToXContentObject, Writeable {

        private final DataCounts dataCounts;

        public Response(String jobId) {
            super(null, null);
            dataCounts = new DataCounts(jobId);
        }

        public Response(DataCounts counts) {
            super(null, null);
            this.dataCounts = counts;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            dataCounts = new DataCounts(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            dataCounts.writeTo(out);
        }

        public DataCounts getDataCounts() {
            return dataCounts;
        }

        @Override
        public RestStatus status() {
            return RestStatus.ACCEPTED;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            dataCounts.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dataCounts);
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

            return Objects.equals(dataCounts, other.dataCounts);

        }
    }

    public static class Request extends JobTaskRequest<Request> {

        public static final ParseField RESET_START = new ParseField("reset_start");
        public static final ParseField RESET_END = new ParseField("reset_end");

        private String resetStart = "";
        private String resetEnd = "";
        private DataDescription dataDescription;
        private XContentType xContentType;
        private BytesReference content;

        public Request(StreamInput in) throws IOException {
            super(in);
            resetStart = in.readOptionalString();
            resetEnd = in.readOptionalString();
            dataDescription = in.readOptionalWriteable(DataDescription::new);
            content = in.readBytesReference();
            if (in.readBoolean()) {
                xContentType = in.readEnum(XContentType.class);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(resetStart);
            out.writeOptionalString(resetEnd);
            out.writeOptionalWriteable(dataDescription);
            out.writeBytesReference(content);
            boolean hasXContentType = xContentType != null;
            out.writeBoolean(hasXContentType);
            if (hasXContentType) {
                out.writeEnum(xContentType);
            }
        }

        public Request(String jobId) {
            super(jobId);
        }

        public String getResetStart() {
            return resetStart;
        }

        public void setResetStart(String resetStart) {
            this.resetStart = resetStart;
        }

        public String getResetEnd() {
            return resetEnd;
        }

        public void setResetEnd(String resetEnd) {
            this.resetEnd = resetEnd;
        }

        public DataDescription getDataDescription() {
            return dataDescription;
        }

        public void setDataDescription(DataDescription dataDescription) {
            this.dataDescription = dataDescription;
        }

        public BytesReference getContent() { return content; }

        public XContentType getXContentType() {
            return xContentType;
        }

        public void setContent(BytesReference content, XContentType xContentType) {
            this.content = content;
            this.xContentType = xContentType;
        }

        @Override
        public int hashCode() {
            // content stream not included
            return Objects.hash(jobId, resetStart, resetEnd, dataDescription, xContentType);
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

            // content stream not included
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(resetStart, other.resetStart) &&
                    Objects.equals(resetEnd, other.resetEnd) &&
                    Objects.equals(dataDescription, other.dataDescription) &&
                    Objects.equals(xContentType, other.xContentType);
        }
    }


}
