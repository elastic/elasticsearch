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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetFieldMappingsIndexRequest extends SingleShardRequest<GetFieldMappingsIndexRequest> {

    private final boolean includeDefaults;
    private final String[] fields;

    private final OriginalIndices originalIndices;

    GetFieldMappingsIndexRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readStringArray();   // former types array
        }
        fields = in.readStringArray();
        includeDefaults = in.readBoolean();
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readBoolean();       // former probablySingleField boolean
        }
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    GetFieldMappingsIndexRequest(GetFieldMappingsRequest other, String index) {
        this.includeDefaults = other.includeDefaults();
        this.fields = other.fields();
        assert index != null;
        this.index(index);
        this.originalIndices = new OriginalIndices(other);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String[] fields() {
        return fields;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        out.writeStringArray(fields);
        out.writeBoolean(includeDefaults);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeBoolean(false);
        }
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

}
