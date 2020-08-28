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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RemoveIndexLifecyclePolicyRequest extends TimedRequest {

    private final List<String> indices;
    private final IndicesOptions indicesOptions;

    public RemoveIndexLifecyclePolicyRequest(List<String> indices) {
        this(indices, IndicesOptions.strictExpandOpen());
    }

    public RemoveIndexLifecyclePolicyRequest(List<String> indices, IndicesOptions indicesOptions) {
        this.indices = Collections.unmodifiableList(Objects.requireNonNull(indices));
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
    }

    public List<String> indices() {
        return indices;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, indicesOptions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RemoveIndexLifecyclePolicyRequest other = (RemoveIndexLifecyclePolicyRequest) obj;
        return Objects.deepEquals(indices, other.indices) &&
                Objects.equals(indicesOptions, other.indicesOptions);
    }
}
