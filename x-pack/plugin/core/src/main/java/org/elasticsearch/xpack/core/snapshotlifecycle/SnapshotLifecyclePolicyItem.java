/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The {@code SnapshotLifecyclePolicyItem} class is a special wrapper almost exactly like the
 * {@link SnapshotLifecyclePolicyMetadata}, however, it elides the headers to ensure that they
 * are not leaked to the user since they may contain sensitive information.
 */
public class SnapshotLifecyclePolicyItem implements ToXContentFragment, Writeable {

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

    // For testing

    SnapshotLifecyclePolicyItem(SnapshotLifecyclePolicy policy, long version, long modifiedDate,
                                SnapshotInvocationRecord lastSuccess, SnapshotInvocationRecord lastFailure) {
        this.policy = policy;
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccess = lastSuccess;
        this.lastFailure = lastFailure;
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

    public SnapshotInvocationRecord getLastSuccess() {
        return lastSuccess;
    }

    public SnapshotInvocationRecord getLastFailure() {
        return lastFailure;
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
        return Objects.hash(policy, version, modifiedDate, lastSuccess, lastFailure);
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
            modifiedDate == other.modifiedDate &&
            Objects.equals(lastSuccess, other.lastSuccess) &&
            Objects.equals(lastFailure, other.lastFailure);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(policy.getId());
        builder.field(SnapshotLifecyclePolicyMetadata.VERSION.getPreferredName(), version);
        builder.timeField(SnapshotLifecyclePolicyMetadata.MODIFIED_DATE_MILLIS.getPreferredName(),
            SnapshotLifecyclePolicyMetadata.MODIFIED_DATE.getPreferredName(), modifiedDate);
        builder.field(SnapshotLifecyclePolicyMetadata.POLICY.getPreferredName(), policy);
        if (lastSuccess != null) {
            builder.field(SnapshotLifecyclePolicyMetadata.LAST_SUCCESS.getPreferredName(), lastSuccess);
        }
        if (lastFailure != null) {
            builder.field(SnapshotLifecyclePolicyMetadata.LAST_FAILURE.getPreferredName(), lastFailure);
        }
        builder.timeField(SnapshotLifecyclePolicyMetadata.NEXT_EXECUTION_MILLIS.getPreferredName(),
            SnapshotLifecyclePolicyMetadata.NEXT_EXECUTION.getPreferredName(), policy.calculateNextExecution());
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
