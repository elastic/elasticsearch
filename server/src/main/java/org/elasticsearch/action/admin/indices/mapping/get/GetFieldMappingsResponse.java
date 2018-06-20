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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/** Response object for {@link GetFieldMappingsRequest} API */
public class GetFieldMappingsResponse extends ActionResponse implements ToXContentFragment {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings = emptyMap();

    GetFieldMappingsResponse(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings) {
        this.mappings = mappings;
    }

    GetFieldMappingsResponse() {
    }

    /** returns the retrieved field mapping. The return map keys are index, type, field (as specified in the request). */
    public Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific field.
     *
     * @param field field name as specified in the {@link GetFieldMappingsRequest}
     * @return FieldMappingMetaData for the requested field or null if not found.
     */
    public FieldMappingMetaData fieldMappings(String index, String type, String field) {
        Map<String, Map<String, FieldMappingMetaData>> indexMapping = mappings.get(index);
        if (indexMapping == null) {
            return null;
        }
        Map<String, FieldMappingMetaData> typeMapping = indexMapping.get(type);
        if (typeMapping == null) {
            return null;
        }
        return typeMapping.get(field);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Map<String, Map<String, FieldMappingMetaData>>> indexEntry : mappings.entrySet()) {
            builder.startObject(indexEntry.getKey());
            builder.startObject(MAPPINGS.getPreferredName());
            for (Map.Entry<String, Map<String, FieldMappingMetaData>> typeEntry : indexEntry.getValue().entrySet()) {
                builder.startObject(typeEntry.getKey());
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

    public static GetFieldMappingsResponse fromXContent(XContentParser parser) throws IOException {
        final Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings = new HashMap<>();
        final Map<String, Object> map = parser.map();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            final Object value = entry.getValue();

            Map<String, Map<String, FieldMappingMetaData>> typeMappings = new HashMap<>();
            mappings.put(entry.getKey(), typeMappings);

            if (value instanceof Map) {
                final Object o = ((Map) value).get(MAPPINGS.getPreferredName());
                if (!(o instanceof Map)) {
                    throw new ParsingException(parser.getTokenLocation(), "Nested " + MAPPINGS.getPreferredName() + " is not found");
                }
                Map<String, Object> map1 = (Map) o;
                for (Map.Entry<String, Object> typeObjectEntry : map1.entrySet()) {
                    Map<String, FieldMappingMetaData> fieldsMapping = new HashMap<>();
                    typeMappings.put(typeObjectEntry.getKey(), fieldsMapping);

                    final Object o1 = typeObjectEntry.getValue();
                    if (!(o1 instanceof Map)) {
                        throw new ParsingException(parser.getTokenLocation(), "Nested type mapping is not found");
                    }
                    Map<String, Object> map2 = (Map) o1;
                    for (Map.Entry<String, Object> e : map2.entrySet()) {
                        final Object o2 = e.getValue();
                        if (!(o2 instanceof Map)) {
                            throw new ParsingException(parser.getTokenLocation(), "Nested field mapping is not found");
                        }
                        Map<String, Object> map3 = (Map) o2;

                        String fullName = (String) map3.get(FieldMappingMetaData.FULL_NAME.getPreferredName());
                        XContentBuilder builder = jsonBuilder();
                        final Map<String, ?> values = (Map<String, ?>) map3.get(FieldMappingMetaData.MAPPING.getPreferredName());
                        builder.map(values);
                        final BytesReference source = BytesReference.bytes(builder);

                        FieldMappingMetaData metaData = new FieldMappingMetaData(fullName, source);
                        fieldsMapping.put(e.getKey(), metaData);
                    }

                }
            }
        }

        return new GetFieldMappingsResponse(mappings);
    }

    public static class FieldMappingMetaData implements ToXContentFragment {
        public static final FieldMappingMetaData NULL = new FieldMappingMetaData("", BytesArray.EMPTY);

        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

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
            return XContentHelper.convertToMap(source, true, XContentType.JSON).v2();
        }

        public boolean isNull() {
            return NULL.fullName().equals(fullName) && NULL.source.length() == source.length();
        }

        //pkg-private for testing
        BytesReference getSource() {
            return source;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("full_name", fullName);
            if (params.paramAsBoolean("pretty", false)) {
                builder.field("mapping", sourceAsMap());
            } else {
                try (InputStream stream = source.streamInput()) {
                    builder.rawField("mapping", stream, XContentType.JSON);
                }
            }
            return builder;
        }

        @Override
        public String toString() {
            return "FieldMappingMetaData{fullName='" + fullName + '\'' + ", source=" + source + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FieldMappingMetaData)) return false;
            FieldMappingMetaData that = (FieldMappingMetaData) o;
            return Objects.equals(fullName, that.fullName) &&
                Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fullName, source);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        Map<String, Map<String, Map<String, FieldMappingMetaData>>> indexMapBuilder = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            int typesSize = in.readVInt();
            Map<String, Map<String, FieldMappingMetaData>> typeMapBuilder = new HashMap<>(typesSize);
            for (int j = 0; j < typesSize; j++) {
                String type = in.readString();
                int fieldSize = in.readVInt();
                Map<String, FieldMappingMetaData> fieldMapBuilder = new HashMap<>(fieldSize);
                for (int k = 0; k < fieldSize; k++) {
                    fieldMapBuilder.put(in.readString(), new FieldMappingMetaData(in.readString(), in.readBytesReference()));
                }
                typeMapBuilder.put(type, unmodifiableMap(fieldMapBuilder));
            }
            indexMapBuilder.put(index, unmodifiableMap(typeMapBuilder));
        }
        mappings = unmodifiableMap(indexMapBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, Map<String, Map<String, FieldMappingMetaData>>> indexEntry : mappings.entrySet()) {
            out.writeString(indexEntry.getKey());
            out.writeVInt(indexEntry.getValue().size());
            for (Map.Entry<String, Map<String, FieldMappingMetaData>> typeEntry : indexEntry.getValue().entrySet()) {
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
