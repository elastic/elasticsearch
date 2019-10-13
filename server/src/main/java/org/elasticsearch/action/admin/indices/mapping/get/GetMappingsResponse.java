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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

public class GetMappingsResponse extends ActionResponse implements ToXContentFragment {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private final ImmutableOpenMap<String, MappingMetaData> mappings;

    public GetMappingsResponse(ImmutableOpenMap<String, MappingMetaData> mappings) {
        this.mappings = mappings;
    }

    GetMappingsResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, MappingMetaData> indexMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            if (in.getVersion().before(Version.V_8_0_0)) {
                int mappingCount = in.readVInt();
                assert mappingCount == 1 || mappingCount == 0 : "Expected 0 or 1 mappings but got " + mappingCount;
                if (mappingCount == 1) {
                    String type = in.readString();
                    assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected type [_doc] but got [" + type + "]";
                    indexMapBuilder.put(index, new MappingMetaData(in));
                }
            }
            else {
                boolean hasMapping = in.readBoolean();
                indexMapBuilder.put(index, hasMapping ? new MappingMetaData(in) : null);
            }
        }
        mappings = indexMapBuilder.build();
    }

    public ImmutableOpenMap<String, MappingMetaData> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, MappingMetaData> getMappings() {
        return mappings();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(mappings.size());
        for (ObjectObjectCursor<String, MappingMetaData> indexEntry : mappings) {
            out.writeString(indexEntry.key);
            if (out.getVersion().before(Version.V_8_0_0)) {
                out.writeVInt(indexEntry.value == null ? 0 : 1);
                if (indexEntry.value != null) {
                    out.writeString(MapperService.SINGLE_MAPPING_NAME);
                    indexEntry.value.writeTo(out);
                }
            }
            else {
                out.writeBoolean(indexEntry.value != null);
                if (indexEntry.value != null) {
                    indexEntry.value.writeTo(out);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        for (final ObjectObjectCursor<String, MappingMetaData> indexEntry : getMappings()) {
            builder.startObject(indexEntry.key);
            {
                MappingMetaData mappings = indexEntry.value;
                if (mappings == null) {
                    // no mappings yet
                    builder.startObject(MAPPINGS.getPreferredName()).endObject();
                } else {
                    builder.field(MAPPINGS.getPreferredName(), mappings.sourceAsMap());
                }
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return mappings.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        GetMappingsResponse other = (GetMappingsResponse) obj;
        return this.mappings.equals(other.mappings);
    }
}
