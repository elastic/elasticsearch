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
package org.elasticsearch.client.migration;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;

import java.util.Objects;

/**
 * A request for performing Upgrade on Index
 * Part of Migration API
 */
public class IndexUpgradeRequest extends TimedRequest {

    private String index;
    private IndicesOptions indicesOptions = IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;

    public IndexUpgradeRequest(String index) {
        this.index = index;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
    }

    public String index() {
        return index;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexUpgradeRequest request = (IndexUpgradeRequest) o;
        return Objects.equals(index, request.index) &&
            Objects.equals(indicesOptions.toString(), request.indicesOptions.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, indicesOptions);
    }
}
