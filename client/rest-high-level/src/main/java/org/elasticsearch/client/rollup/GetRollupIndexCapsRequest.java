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
package org.elasticsearch.client.rollup;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Objects;

public class GetRollupIndexCapsRequest implements Validatable {

    private String[] indices;
    private IndicesOptions options;

    public GetRollupIndexCapsRequest(final String... indices) {
        this(indices, IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED);
    }

    public GetRollupIndexCapsRequest(final String[] indices, final IndicesOptions options) {
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("[indices] must not be null or empty");
        }
        for (String index : indices) {
            if (Strings.isNullOrEmpty(index)) {
                throw new IllegalArgumentException("[index] must not be null or empty");
            }
        }
        this.indices = indices;
        this.options = Objects.requireNonNull(options);
    }

    public IndicesOptions indicesOptions() {
        return options;
    }

    public String[] indices() {
        return indices;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), options);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetRollupIndexCapsRequest other = (GetRollupIndexCapsRequest) obj;
        return Arrays.equals(indices, other.indices)
            && Objects.equals(options, other.options);
    }
}
