/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 */
public class GetMappingsResponse extends ActionResponse {

    private ImmutableMap<String, ImmutableMap<String, MappingMetaData>> mappings = ImmutableMap.of();

    GetMappingsResponse(ImmutableMap<String, ImmutableMap<String, MappingMetaData>> mappings) {
        this.mappings = mappings;
    }

    GetMappingsResponse() {
    }

    public ImmutableMap<String, ImmutableMap<String, MappingMetaData>> mappings() {
        return mappings;
    }

    public ImmutableMap<String, ImmutableMap<String, MappingMetaData>> getMappings() {
        return mappings();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableMap.Builder<String, ImmutableMap<String, MappingMetaData>> indexMapBuilder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableMap.Builder<String, MappingMetaData> typeMapBuilder = ImmutableMap.builder();
            for (int j = 0; j < valueSize; j++) {
                typeMapBuilder.put(in.readString(), MappingMetaData.readFrom(in));
            }
            indexMapBuilder.put(key, typeMapBuilder.build());
        }
        mappings = indexMapBuilder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, ImmutableMap<String, MappingMetaData>> indexEntry : mappings.entrySet()) {
            out.writeString(indexEntry.getKey());
            out.writeVInt(indexEntry.getValue().size());
            for (Map.Entry<String, MappingMetaData> typeEntry : indexEntry.getValue().entrySet()) {
                out.writeString(typeEntry.getKey());
                MappingMetaData.writeTo(typeEntry.getValue(), out);
            }
        }
    }
}
