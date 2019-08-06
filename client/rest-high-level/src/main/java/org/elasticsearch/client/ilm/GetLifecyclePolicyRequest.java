/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
