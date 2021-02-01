/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class PersistJobAction extends ActionType<PersistJobAction.Response> {

    public static final PersistJobAction INSTANCE = new PersistJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/persist";

    private PersistJobAction() {
        super(NAME, PersistJobAction.Response::new);
    }

    public static class Request extends JobTaskRequest<PersistJobAction.Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
            // isBackground for fwc
            in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            // isBackground for fwc
            out.writeBoolean(true);
        }

        public Request(String jobId) {
            super(jobId);
        }

        public boolean isBackGround() {
            return true;
        }

        public boolean isForeground() {
            return !isBackGround();
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, isBackGround());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PersistJobAction.Request other = (PersistJobAction.Request) obj;
            return Objects.equals(jobId, other.jobId) && this.isBackGround() == other.isBackGround();
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private final boolean persisted;

        public Response(boolean persisted) {
            super(null, null);
            this.persisted = persisted;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            persisted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(persisted);
        }

        public boolean isPersisted() {
            return persisted;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return this.persisted == that.persisted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(persisted);
        }
    }

}
