/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StopDataFrameTransformAction extends ActionType<StopDataFrameTransformAction.Response> {

    public static final StopDataFrameTransformAction INSTANCE = new StopDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/stop";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private StopDataFrameTransformAction() {
        super(NAME, StopDataFrameTransformAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> {
        private final String id;
        private final boolean waitForCompletion;
        private final boolean force;
        private final boolean allowNoMatch;
        private Set<String> expandedIds;

        public Request(String id, boolean waitForCompletion, boolean force, @Nullable TimeValue timeout, boolean allowNoMatch) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameField.ID.getPreferredName());
            this.waitForCompletion = waitForCompletion;
            this.force = force;

            // use the timeout value already present in BaseTasksRequest
            this.setTimeout(timeout == null ? DEFAULT_TIMEOUT : timeout);
            this.allowNoMatch = allowNoMatch;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            waitForCompletion = in.readBoolean();
            force = in.readBoolean();
            if (in.readBoolean()) {
                expandedIds = new HashSet<>(Arrays.asList(in.readStringArray()));
            }
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                this.allowNoMatch = in.readBoolean();
            } else {
                this.allowNoMatch = true;
            }
        }

        public String getId() {
            return id;
        }

        public boolean waitForCompletion() {
            return waitForCompletion;
        }

        public boolean isForce() {
            return force;
        }

        public Set<String> getExpandedIds() {
            return expandedIds;
        }

        public void setExpandedIds(Set<String> expandedIds ) {
            this.expandedIds = expandedIds;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(waitForCompletion);
            out.writeBoolean(force);
            boolean hasExpandedIds = expandedIds != null;
            out.writeBoolean(hasExpandedIds);
            if (hasExpandedIds) {
                out.writeStringArray(expandedIds.toArray(new String[0]));
            }
            if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
                out.writeBoolean(allowNoMatch);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(id, waitForCompletion, force, expandedIds, this.getTimeout(), allowNoMatch);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;

            // the base class does not implement equals, therefore we need to compare timeout ourselves
            if (Objects.equals(this.getTimeout(), other.getTimeout()) == false) {
                return false;
            }

            return Objects.equals(id, other.id) &&
                    Objects.equals(waitForCompletion, other.waitForCompletion) &&
                    Objects.equals(force, other.force) &&
                    Objects.equals(expandedIds, other.expandedIds) &&
                    allowNoMatch == other.allowNoMatch;
        }

        @Override
        public boolean match(Task task) {
            if (task.getDescription().startsWith(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX)) {
                String id = task.getDescription().substring(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX.length());
                if (expandedIds != null) {
                    return expandedIds.contains(id);
                }
            }

            return false;
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean acknowledged;

        public Response(StreamInput in) throws IOException {
            super(in);
            acknowledged = in.readBoolean();
        }

        public Response(boolean acknowledged) {
            super(Collections.emptyList(), Collections.emptyList());
            this.acknowledged = acknowledged;
        }

        public Response(List<TaskOperationFailure> taskFailures,
                        List<? extends ElasticsearchException> nodeFailures,
                        boolean acknowledged) {
            super(taskFailures, nodeFailures);
            this.acknowledged = acknowledged;
        }

        public boolean isAcknowledged() {
            return acknowledged;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(acknowledged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("acknowledged", acknowledged);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Response response = (Response) o;
            return acknowledged == response.acknowledged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged);
        }
    }
}
