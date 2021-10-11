/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PutSnapshotLifecyclePolicyRequest extends TimedRequest implements ToXContentObject {

    private final SnapshotLifecyclePolicy policy;

    public PutSnapshotLifecyclePolicyRequest(SnapshotLifecyclePolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy definition cannot be null");
    }

    public SnapshotLifecyclePolicy getPolicy() {
        return policy;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        policy.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutSnapshotLifecyclePolicyRequest other = (PutSnapshotLifecyclePolicyRequest) o;
        return Objects.equals(this.policy, other.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.policy);
    }
}
