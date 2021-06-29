/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.client.TimedRequest;

import java.util.Arrays;

public class GetSnapshotLifecyclePolicyRequest extends TimedRequest {
    private final String[] policyIds;

    public GetSnapshotLifecyclePolicyRequest(String... ids) {
        this.policyIds = ids;
    }

    public String[] getPolicyIds() {
        return this.policyIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSnapshotLifecyclePolicyRequest other = (GetSnapshotLifecyclePolicyRequest) o;
        return Arrays.equals(this.policyIds, other.policyIds);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.policyIds);
    }
}
