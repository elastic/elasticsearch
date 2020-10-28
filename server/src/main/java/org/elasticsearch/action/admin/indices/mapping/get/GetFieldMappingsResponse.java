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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * Response object for {@link GetFieldMappingsRequest} API
 *
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should go to that client class as well.
 */
public class GetFieldMappingsResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private final Map<String, Map<String, FieldMappingMetadata>> mappings;

    GetFieldMappingsResponse(Map<String, Map<String, FieldMappingMetadata>> mappings) {
        this.mappings = mappings;
    }

    GetFieldMappingsResponse(StreamInput in) throws IOException {
        super(in);
        mappings = unmodifiableMap(in.readMap(StreamInput::readString, mapIn -> {
            if (mapIn.getVersion().before(Version.V_8_0_0)) {
                int typesSize = mapIn.readVInt();
                assert typesSize == 1 || typesSize == 0 : "Expected 0 or 1 types but got " + typesSize;
                if (typesSize == 0) {
                    return Collections.emptyMap();
                }
                mapIn.readString(); // type
            }
            return unmodifiableMap(mapIn.readMap(StreamInput::readString,
                    inpt -> new FieldMappingMetadata(inpt.readString(), inpt.readBytesReference())));
        }));
    }

    /** returns the retrieved field mapping. The return map keys are index, field (as specified in the request). */
    public Map<String, Map<String, FieldMappingMetadata>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific field.
     *
     * @param field field name as specified in the {@link GetFieldMappingsRequest}
     * @return FieldMappingMetadata for the requested field or null if not found.
     */
    public FieldMappingMetadata fieldMappings(String index, String field) {
        Map<String, FieldMappingMetadata> indexMapping = mappings.get(index);
        if (indexMapping == null) {
            return null;
        }
        return indexMapping.get(field);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Map<String, FieldMappingMetadata>> indexEntry : mappings.entrySet()) {
            builder.startObject(indexEntry.getKey());
            builder.startObject(MAPPINGS.getPreferredName());

            if (indexEntry.getValue() != null) {
                addFieldMappingsToBuilder(builder, params, indexEntry.getValue());
            }

            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void addFieldMappingsToBuilder(XContentBuilder builder,
                                           Params params,
                                           Map<String, FieldMappingMetadata> mappings) throws IOException {
        for (Map.Entry<String, FieldMappingMetadata> fieldEntry : mappings.entrySet()) {
            builder.startObject(fieldEntry.getKey());
            fieldEntry.getValue().toXContent(builder, params);
            builder.endObject();
        }
    }

    public static class FieldMappingMetadata implements ToXContentFragment {

        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

        private static final ConstructingObjectParser<FieldMappingMetadata, String> PARSER =
            new ConstructingObjectParser<>("field_mapping_meta_data", true,
                a -> new FieldMappingMetadata((String)a[0], (BytesReference)a[1])
            );

        private final String fullName;
        private final BytesReference source;

        public FieldMappingMetadata(String fullName, BytesReference source) {
            this.fullName = fullName;
            this.source = source;
        }

        public String fullName() {
            return fullName;
        }

        /** Returns the mappings as a map. Note that the returned map has a single key which is always the field's {@link Mapper#name}. */
        public Map<String, Object> sourceAsMap() {
            return XContentHelper.convertToMap(source, true, XContentType.JSON).v2();
        }

        //pkg-private for testing
        BytesReference getSource() {
            return source;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(FULL_NAME.getPreferredName(), fullName);
            if (params.paramAsBoolean("pretty", false)) {
                builder.field("mapping", sourceAsMap());
            } else {
                try (InputStream stream = source.streamInput()) {
                    builder.rawField(MAPPING.getPreferredName(), stream, XContentType.JSON);
                }
            }
            return builder;
        }

        @Override
        public String toString() {
            return "FieldMappingMetadata{fullName='" + fullName + '\'' + ", source=" + source + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FieldMappingMetadata)) return false;
            FieldMappingMetadata that = (FieldMappingMetadata) o;
            return Objects.equals(fullName, that.fullName) &&
                Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fullName, source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(mappings, StreamOutput::writeString, (outpt, map) -> {
            if (outpt.getVersion().before(Version.V_8_0_0)) {
                outpt.writeVInt(1);
                outpt.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            outpt.writeMap(map, StreamOutput::writeString, (o, v) -> {
                o.writeString(v.fullName());
                o.writeBytesReference(v.source);
            });
        });
    }

    @Override
    public String toString() {
        return "GetFieldMappingsResponse{" +
            "mappings=" + mappings +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GetFieldMappingsResponse)) return false;
        GetFieldMappingsResponse that = (GetFieldMappingsResponse) o;
        return Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings);
    }

}
