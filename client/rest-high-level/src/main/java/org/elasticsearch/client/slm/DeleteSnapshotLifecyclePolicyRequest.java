/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.client.TimedRequest;

import java.util.Objects;

public class DeleteSnapshotLifecyclePolicyRequest extends TimedRequest {
    private final String policyId;

    public DeleteSnapshotLifecyclePolicyRequest(String policyId) {
        this.policyId = policyId;
    }

    public String getPolicyId() {
        return this.policyId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteSnapshotLifecyclePolicyRequest other = (DeleteSnapshotLifecyclePolicyRequest) o;
        return this.policyId.equals(other.policyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.policyId);
    }
}
