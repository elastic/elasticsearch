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

import java.util.Arrays;

public class GetLifecyclePolicyRequest extends TimedRequest {

    private final String[] policyNames;

    public GetLifecyclePolicyRequest(String... policyNames) {
        if (policyNames == null) {
            this.policyNames = Strings.EMPTY_ARRAY;
        } else {
            for (String name : policyNames) {
                if (name == null) {
                    throw new IllegalArgumentException("cannot include null policy name");
                }
            }
            this.policyNames = policyNames;
        }
    }

    public String[] getPolicyNames() {
        return policyNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetLifecyclePolicyRequest request = (GetLifecyclePolicyRequest) o;
        return Arrays.equals(getPolicyNames(), request.getPolicyNames());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getPolicyNames());
    }
}
