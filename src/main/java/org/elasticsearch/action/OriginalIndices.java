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

package org.elasticsearch.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Used to keep track of original indices within internal (e.g. shard level) requests
 */
public class OriginalIndices implements IndicesRequest {

    private final String[] indices;
    private final IndicesOptions indicesOptions;
    private final boolean missing;

    public OriginalIndices() {
        this.missing = true;
        this.indices = null;
        this.indicesOptions = null;
    }

    public OriginalIndices(IndicesRequest indicesRequest) {
        this.missing = false;
        this.indices = indicesRequest.indices();
        this.indicesOptions = indicesRequest.indicesOptions();
    }

    public OriginalIndices(String[] indices, IndicesOptions indicesOptions) {
        this.missing = false;
        this.indices = indices;
        this.indicesOptions = indicesOptions;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public static OriginalIndices readOptionalOriginalIndices(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_1_4_0)) {
            boolean missing = in.readBoolean();
            if (!missing) {
                return new OriginalIndices(in.readStringArray(), IndicesOptions.readIndicesOptions(in));
            }
        }
        return new OriginalIndices();
    }

    public static void writeOptionalOriginalIndices(OriginalIndices originalIndices, StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_1_4_0)) {
            out.writeBoolean(originalIndices.missing);
            if (!originalIndices.missing) {
                out.writeStringArrayNullable(originalIndices.indices);
                originalIndices.indicesOptions.writeIndicesOptions(out);
            }
        }
    }

    public static OriginalIndices readOriginalIndices(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_1_4_0)) {
            return new OriginalIndices(in.readStringArray(), IndicesOptions.readIndicesOptions(in));
        }
        return new OriginalIndices();
    }


    public static void writeOriginalIndices(OriginalIndices originalIndices, StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_1_4_0)) {
            out.writeStringArrayNullable(originalIndices.indices);
            originalIndices.indicesOptions.writeIndicesOptions(out);
        }
    }
}
