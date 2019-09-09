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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.elasticsearch.client.TimedRequest;

public class RetryLifecyclePolicyRequest extends TimedRequest {

    private final List<String> indices;

    public RetryLifecyclePolicyRequest(String... indices) {
        if (indices.length == 0) {
            throw new IllegalArgumentException("Must at least specify one index to retry");
        }
        this.indices = Arrays.asList(indices);
    }

    public List<String> getIndices() {
        return indices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetryLifecyclePolicyRequest that = (RetryLifecyclePolicyRequest) o;
        return indices.size() == that.indices.size() && indices.containsAll(that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices);
    }
}
