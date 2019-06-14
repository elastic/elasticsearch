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

package org.elasticsearch.action.admin.indices.reloadanalyzer;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;

import java.util.Arrays;
import java.util.Objects;

/**
 * Request for reloading index search analyzers
 */
public class ReloadAnalyzersRequest extends BroadcastRequest<ReloadAnalyzersRequest> {

    /**
     * Constructs a new request for reloading index search analyzers for one or more indices
     */
    public ReloadAnalyzersRequest(String... indices) {
        super(indices);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReloadAnalyzersRequest that = (ReloadAnalyzersRequest) o;
        return Objects.equals(indicesOptions(), that.indicesOptions())
                && Arrays.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions(), Arrays.hashCode(indices));
    }

}
