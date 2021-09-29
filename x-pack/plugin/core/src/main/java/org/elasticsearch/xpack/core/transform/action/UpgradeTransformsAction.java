/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class UpgradeTransformsAction extends ActionType<UpgradeTransformsAction.Response> {

    public static final UpgradeTransformsAction INSTANCE = new UpgradeTransformsAction();
    public static final String NAME = "cluster:admin/transform/upgrade";

    private UpgradeTransformsAction() {
        super(NAME, UpgradeTransformsAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        // todo: expose for REST?
        private final boolean dryRun;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dryRun = in.readBoolean();
        }

        public Request(boolean dryRun) {
            super();
            this.dryRun = dryRun;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(dryRun);
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {
        private final boolean success;
        private final Long updated;
        private final Long noAction;
        private final Long needsUpdate;

        public Response(StreamInput in) throws IOException {
            super(in);
            success = in.readBoolean();
            updated = in.readOptionalVLong();
            noAction = in.readOptionalVLong();
            needsUpdate = in.readOptionalVLong();
        }

        public Response(boolean success, Long updated, Long noAction, Long needsUpdate) {
            super(Collections.emptyList(), Collections.emptyList());
            this.success = success;
            this.updated = updated;
            this.noAction = noAction;
            this.needsUpdate = needsUpdate;
        }

        public boolean isSuccess() {
            return success;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(success);
            out.writeOptionalVLong(updated);
            out.writeOptionalVLong(noAction);
            out.writeOptionalVLong(needsUpdate);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("success", success);
            if (updated != null) {
                builder.field("updated", updated);
            }
            if (noAction != null) {
                builder.field("no_action", noAction);
            }
            if (needsUpdate != null) {
                builder.field("needs_pdate", needsUpdate);
            }
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
            return success == response.success
                && this.updated == response.updated
                && this.noAction == response.noAction
                && this.needsUpdate == response.needsUpdate;
        }

        @Override
        public int hashCode() {
            return Objects.hash(success, updated, noAction, needsUpdate);
        }
    }
}
