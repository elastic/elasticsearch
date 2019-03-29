/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GetSnapshotLifecycleAction extends Action<GetSnapshotLifecycleAction.Response> {
    public static final GetSnapshotLifecycleAction INSTANCE = new GetSnapshotLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/snapshot/get";

    protected GetSnapshotLifecycleAction() {
        super(NAME);
    }

    @Override
    public GetSnapshotLifecycleAction.Response newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<GetSnapshotLifecycleAction.Response> getResponseReader() {
        return GetSnapshotLifecycleAction.Response::new;
    }

    public static class Request extends AcknowledgedRequest<GetSnapshotLifecycleAction.Request> {

        private String[] lifecycleIds;

        public Request(String... lifecycleIds) {
            this.lifecycleIds = Objects.requireNonNull(lifecycleIds, "ids may not be null");
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
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            lifecycleIds = in.readStringArray();
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

    /**
     * The {@code SnapshotLifecyclePolicyItem} class is a special wrapper almost exactly like the
     * {@link SnapshotLifecyclePolicyMetadata}, however, it elides the headers to ensure that they
     * are not leaked to the user since they may contain sensitive information.
     */
    public static class SnapshotLifecyclePolicyItem implements ToXContentFragment, Writeable {

        private final SnapshotLifecyclePolicy policy;
        private final long version;
        private final long modifiedDate;
        @Nullable
        private final SnapshotInvocationRecord lastSuccess;
        @Nullable
        private final SnapshotInvocationRecord lastFailure;

        public SnapshotLifecyclePolicyItem(SnapshotLifecyclePolicyMetadata policyMetadata) {
            this.policy = policyMetadata.getPolicy();
            this.version = policyMetadata.getVersion();
            this.modifiedDate = policyMetadata.getModifiedDate();
            this.lastSuccess = policyMetadata.getLastSuccess();
            this.lastFailure = policyMetadata.getLastFailure();
        }

        public SnapshotLifecyclePolicyItem(StreamInput in) throws IOException {
            this.policy = new SnapshotLifecyclePolicy(in);
            this.version = in.readVLong();
            this.modifiedDate = in.readVLong();
            this.lastSuccess = in.readOptionalWriteable(SnapshotInvocationRecord::new);
            this.lastFailure = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        }

        public SnapshotLifecyclePolicy getPolicy() {
            return policy;
        }

        public long getVersion() {
            return version;
        }

        public long getModifiedDate() {
            return modifiedDate;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            policy.writeTo(out);
            out.writeVLong(version);
            out.writeVLong(modifiedDate);
            out.writeOptionalWriteable(lastSuccess);
            out.writeOptionalWriteable(lastFailure);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy, version, modifiedDate);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            SnapshotLifecyclePolicyItem other = (SnapshotLifecyclePolicyItem) obj;
            return policy.equals(other.policy) &&
                version == other.version &&
                modifiedDate == other.modifiedDate;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(policy.getId());
            builder.field("version", version);
            builder.field("modified_date", modifiedDate);
            builder.field("policy", policy);
            if (lastSuccess != null) {
                builder.field("last_success", lastSuccess);
            }
            if (lastFailure != null) {
                builder.field("last_failure", lastFailure);
            }
            builder.endObject();
            return builder;
        }
    }
}
