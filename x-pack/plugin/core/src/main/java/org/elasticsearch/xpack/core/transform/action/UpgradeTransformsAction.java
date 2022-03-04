/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class UpgradeTransformsAction extends ActionType<UpgradeTransformsAction.Response> {

    public static final UpgradeTransformsAction INSTANCE = new UpgradeTransformsAction();
    public static final String NAME = "cluster:admin/transform/upgrade";

    private UpgradeTransformsAction() {
        super(NAME, UpgradeTransformsAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final boolean dryRun;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dryRun = in.readBoolean();
        }

        public Request(boolean dryRun, TimeValue timeout) {
            super(timeout);
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

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(timeout(), dryRun);
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

            // the base class does not implement equals, therefore we need to check timeout ourselves
            return this.dryRun == other.dryRun && timeout().equals(other.timeout());
        }
    }

    public static class Response extends ActionResponse implements Writeable, ToXContentObject {

        private final long updated;
        private final long noAction;
        private final long needsUpdate;

        public Response(StreamInput in) throws IOException {
            updated = in.readVLong();
            noAction = in.readVLong();
            needsUpdate = in.readVLong();
        }

        public Response(long updated, long noAction, long needsUpdate) {
            if (updated < 0 || noAction < 0 || needsUpdate < 0) {
                throw new IllegalArgumentException("response counters must be > 0");
            }

            this.updated = updated;
            this.noAction = noAction;
            this.needsUpdate = needsUpdate;
        }

        public long getUpdated() {
            return updated;
        }

        public long getNoAction() {
            return noAction;
        }

        public long getNeedsUpdate() {
            return needsUpdate;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(updated);
            out.writeVLong(noAction);
            out.writeVLong(needsUpdate);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("updated", updated);
            builder.field("no_action", noAction);
            builder.field("needs_update", needsUpdate);
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
            Response other = (Response) obj;
            return this.updated == other.updated && this.noAction == other.noAction && this.needsUpdate == other.needsUpdate;
        }

        @Override
        public int hashCode() {
            return Objects.hash(updated, noAction, needsUpdate);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }
}
