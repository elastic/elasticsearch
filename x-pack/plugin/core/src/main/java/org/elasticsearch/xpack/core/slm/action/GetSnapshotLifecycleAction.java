/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GetSnapshotLifecycleAction extends ActionType<GetSnapshotLifecycleAction.Response> {
    public static final GetSnapshotLifecycleAction INSTANCE = new GetSnapshotLifecycleAction();
    public static final String NAME = "cluster:admin/slm/get";

    protected GetSnapshotLifecycleAction() {
        super(NAME, GetSnapshotLifecycleAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<GetSnapshotLifecycleAction.Request> {

        private String[] lifecycleIds;

        public Request(String... lifecycleIds) {
            this.lifecycleIds = Objects.requireNonNull(lifecycleIds, "ids may not be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            lifecycleIds = in.readStringArray();
        }

        public Request() {
            this.lifecycleIds = Strings.EMPTY_ARRAY;
        }

        public String[] getLifecycleIds() {
            return this.lifecycleIds;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(lifecycleIds);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(lifecycleIds);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.equals(lifecycleIds, other.lifecycleIds);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private List<SnapshotLifecyclePolicyItem> lifecycles;

        public Response() { }

        public Response(List<SnapshotLifecyclePolicyItem> lifecycles) {
            this.lifecycles = lifecycles;
        }

        public Response(StreamInput in) throws IOException {
            this.lifecycles = in.readList(SnapshotLifecyclePolicyItem::new);
        }

        public List<SnapshotLifecyclePolicyItem> getPolicies() {
            return this.lifecycles;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (SnapshotLifecyclePolicyItem item : lifecycles) {
                item.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(lifecycles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lifecycles);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return lifecycles.equals(other.lifecycles);
        }
    }

}
