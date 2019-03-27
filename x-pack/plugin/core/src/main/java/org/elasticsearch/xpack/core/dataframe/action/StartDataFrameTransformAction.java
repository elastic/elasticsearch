/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class StartDataFrameTransformAction extends Action<StartDataFrameTransformAction.Response> {

    public static final StartDataFrameTransformAction INSTANCE = new StartDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/start";

    private StartDataFrameTransformAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String id;
        private boolean force;

        public Request(String id, boolean force) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameField.ID.getPreferredName());
            this.force = force;
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
        }

        public String getId() {
            return id;
        }

        public boolean isForce() {
            return force;
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

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {
        private boolean started;

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            readFrom(in);
        }

        public Response(boolean started) {
            super(Collections.emptyList(), Collections.emptyList());
            this.started = started;
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            started = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(started);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("started", started);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response response = (Response) obj;
            return started == response.started;
        }

        @Override
        public int hashCode() {
            return Objects.hash(started);
        }
    }
}
