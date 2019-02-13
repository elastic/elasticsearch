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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Objects;

/**
 * A request for retrieving upgrade information
 * Part of Migration API
 */
public class IndexUpgradeInfoRequest extends TimedRequest implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true);

    public IndexUpgradeInfoRequest(String... indices) {
        indices(indices);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndexUpgradeInfoRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices, "indices cannot be null");
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexUpgradeInfoRequest request = (IndexUpgradeInfoRequest) o;
        return Arrays.equals(indices, request.indices) &&
                Objects.equals(indicesOptions.toString(), request.indicesOptions.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions.toString());
    }
}
