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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.Map;

/** Response object for {@link GetFieldMappingsRequest} API */
public class GetFieldMappingsResponse extends ActionResponse implements ToXContent {

    private ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappings = ImmutableMap.of();

    GetFieldMappingsResponse(ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappings) {
        this.mappings = mappings;
    }

    GetFieldMappingsResponse() {
    }

    /** returns the retrieved field mapping. The return map keys are index, type, field (as specified in the request). */
    public ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific field.
     *
     * @param field field name as specified in the {@link GetFieldMappingsRequest}
     * @return FieldMappingMetaData for the requested field or null if not found.
     */
    public FieldMappingMetaData fieldMappings(String index, String type, String field) {
        ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>> indexMapping = mappings.get(index);
        if (indexMapping == null) {
            return null;
        }
        ImmutableMap<String, FieldMappingMetaData> typeMapping = indexMapping.get(type);
        if (typeMapping == null) {
            return null;
        }
        return typeMapping.get(field);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexEntry : mappings.entrySet()) {
            builder.startObject(indexEntry.getKey(), XContentBuilder.FieldCaseConversion.NONE);
            builder.startObject("mappings");
            for (Map.Entry<String, ImmutableMap<String, FieldMappingMetaData>> typeEntry : indexEntry.getValue().entrySet()) {
                builder.startObject(typeEntry.getKey(), XContentBuilder.FieldCaseConversion.NONE);
                for (Map.Entry<String, FieldMappingMetaData> fieldEntry : typeEntry.getValue().entrySet()) {
                    builder.startObject(fieldEntry.getKey());
                    fieldEntry.getValue().toXContent(builder, params);
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }
        return builder;
    }

    public static class FieldMappingMetaData implements ToXContent {
        public static final FieldMappingMetaData NULL = new FieldMappingMetaData("", BytesArray.EMPTY);

        private String fullName;
        private BytesReference source;

        public FieldMappingMetaData(String fullName, BytesReference source) {
            this.fullName = fullName;
            this.source = source;
        }

        public String fullName() {
            return fullName;
        }

        /** Returns the mappings as a map. Note that the returned map has a single key which is always the field's {@link Mapper#name}. */
        public Map<String, Object> sourceAsMap() {
            return XContentHelper.convertToMap(source, true).v2();
        }

        public boolean isNull() {
            return NULL.fullName().equals(fullName) && NULL.source.length() == source.length();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("full_name", fullName);
            if (params.paramAsBoolean("pretty", false)) {
                builder.field("mapping", sourceAsMap());
            } else {
                builder.rawField("mapping", source);
            }
            return builder;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableMap.Builder<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexMapBuilder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            int typesSize = in.readVInt();
            ImmutableMap.Builder<String, ImmutableMap<String, FieldMappingMetaData>> typeMapBuilder = ImmutableMap.builder();
            for (int j = 0; j < typesSize; j++) {
                String type = in.readString();
                ImmutableMap.Builder<String, FieldMappingMetaData> fieldMapBuilder = ImmutableMap.builder();
                int fieldSize = in.readVInt();
                for (int k = 0; k < fieldSize; k++) {
                    fieldMapBuilder.put(in.readString(), new FieldMappingMetaData(in.readString(), in.readBytesReference()));
                }
                typeMapBuilder.put(type, fieldMapBuilder.build());
            }
            indexMapBuilder.put(index, typeMapBuilder.build());
        }
        mappings = indexMapBuilder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexEntry : mappings.entrySet()) {
            out.writeString(indexEntry.getKey());
            out.writeVInt(indexEntry.getValue().size());
            for (Map.Entry<String, ImmutableMap<String, FieldMappingMetaData>> typeEntry : indexEntry.getValue().entrySet()) {
                out.writeString(typeEntry.getKey());
                out.writeVInt(typeEntry.getValue().size());
                for (Map.Entry<String, FieldMappingMetaData> fieldEntry : typeEntry.getValue().entrySet()) {
                    out.writeString(fieldEntry.getKey());
                    FieldMappingMetaData fieldMapping = fieldEntry.getValue();
                    out.writeString(fieldMapping.fullName());
                    out.writeBytesReference(fieldMapping.source);
                }
            }
        }
    }
}
