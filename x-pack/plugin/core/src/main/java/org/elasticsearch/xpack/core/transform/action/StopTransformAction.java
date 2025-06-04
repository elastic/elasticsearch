/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;

public class StopTransformAction extends ActionType<StopTransformAction.Response> {

    public static final StopTransformAction INSTANCE = new StopTransformAction();
    public static final String NAME = "cluster:admin/transform/stop";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private StopTransformAction() {
        super(NAME);
    }

    public static final class Request extends BaseTasksRequest<Request> {
        private final String id;
        private final boolean waitForCompletion;
        private final boolean force;
        private final boolean allowNoMatch;
        private final boolean waitForCheckpoint;
        private Set<String> expandedIds;

        public Request(
            String id,
            boolean waitForCompletion,
            boolean force,
            @Nullable TimeValue timeout,
            boolean allowNoMatch,
            boolean waitForCheckpoint
        ) {
            this.id = ExceptionsHelper.requireNonNull(id, TransformField.ID.getPreferredName());
            this.waitForCompletion = waitForCompletion;
            this.force = force;

            // use the timeout value already present in BaseTasksRequest
            this.setTimeout(timeout == null ? DEFAULT_TIMEOUT : timeout);
            this.allowNoMatch = allowNoMatch;
            this.waitForCheckpoint = waitForCheckpoint;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            waitForCompletion = in.readBoolean();
            force = in.readBoolean();
            if (in.readBoolean()) {
                expandedIds = new HashSet<>(Arrays.asList(in.readStringArray()));
            }
            this.allowNoMatch = in.readBoolean();
            this.waitForCheckpoint = in.readBoolean();
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

        public void setExpandedIds(Set<String> expandedIds) {
            this.expandedIds = expandedIds;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        public boolean isWaitForCheckpoint() {
            return waitForCheckpoint;
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
                out.writeStringCollection(expandedIds);
            }
            out.writeBoolean(allowNoMatch);
            out.writeBoolean(waitForCheckpoint);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (force && waitForCheckpoint) {
                return addValidationError(
                    format("cannot set both [%s] and [%s] to true", TransformField.FORCE, TransformField.WAIT_FOR_CHECKPOINT),
                    null
                );
            }
            return null;
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(id, waitForCompletion, force, expandedIds, this.getTimeout(), allowNoMatch, waitForCheckpoint);
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

            return Objects.equals(id, other.id)
                && Objects.equals(waitForCompletion, other.waitForCompletion)
                && Objects.equals(force, other.force)
                && Objects.equals(expandedIds, other.expandedIds)
                && Objects.equals(waitForCheckpoint, other.waitForCheckpoint)
                && allowNoMatch == other.allowNoMatch;
        }

        @Override
        public boolean match(Task task) {
            if (task.getDescription().startsWith(TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX)) {
                String taskId = task.getDescription().substring(TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX.length());
                if (expandedIds != null) {
                    return expandedIds.contains(taskId);
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

        public Response(
            List<TaskOperationFailure> taskFailures,
            List<? extends ElasticsearchException> nodeFailures,
            boolean acknowledged
        ) {
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return acknowledged == response.acknowledged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged);
        }
    }
}
