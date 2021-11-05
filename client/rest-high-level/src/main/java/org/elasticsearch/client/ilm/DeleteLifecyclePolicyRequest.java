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

import java.util.Objects;

public class DeleteLifecyclePolicyRequest extends TimedRequest {

    private final String lifecyclePolicy;

    public DeleteLifecyclePolicyRequest(String lifecyclePolicy) {
        if (Strings.isNullOrEmpty(lifecyclePolicy)) {
            throw new IllegalArgumentException("lifecycle name must be present");
        }
        this.lifecyclePolicy = lifecyclePolicy;
    }

    public String getLifecyclePolicy() {
        return lifecyclePolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeleteLifecyclePolicyRequest that = (DeleteLifecyclePolicyRequest) o;
        return Objects.equals(getLifecyclePolicy(), that.getLifecyclePolicy());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLifecyclePolicy());
    }
}
