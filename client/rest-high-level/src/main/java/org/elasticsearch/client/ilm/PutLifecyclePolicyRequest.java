/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PutLifecyclePolicyRequest extends TimedRequest implements ToXContentObject {

    private final LifecyclePolicy policy;

    public PutLifecyclePolicyRequest(LifecyclePolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("policy definition cannot be null");
        }
        if (Strings.isNullOrEmpty(policy.getName())) {
            throw new IllegalArgumentException("policy name must be present");
        }
        this.policy = policy;
    }

    public String getName() {
        return policy.getName();
    }

    public LifecyclePolicy getLifecyclePolicy() {
        return policy;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("policy", policy);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutLifecyclePolicyRequest that = (PutLifecyclePolicyRequest) o;
        return Objects.equals(getLifecyclePolicy(), that.getLifecyclePolicy());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLifecyclePolicy());
    }
}
