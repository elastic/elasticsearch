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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;

/**
 * Response object for {@link GetFieldMappingsRequest} API
 *
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should go to that client class as well.
 */
public class GetFieldMappingsResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private static final ObjectParser<Map<String, Map<String, FieldMappingMetaData>>, String> PARSER =
        new ObjectParser<>(MAPPINGS.getPreferredName(), true, HashMap::new);

    static {
        PARSER.declareField((p, typeMappings, index) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String typeName = p.currentName();

                if (p.nextToken() == XContentParser.Token.START_OBJECT) {
                    final Map<String, FieldMappingMetaData> typeMapping = new HashMap<>();
                    typeMappings.put(typeName, typeMapping);

                    while (p.nextToken() == XContentParser.Token.FIELD_NAME) {
                        final String fieldName = p.currentName();
                        final FieldMappingMetaData fieldMappingMetaData = FieldMappingMetaData.fromXContent(p);
                        typeMapping.put(fieldName, fieldMappingMetaData);
                    }
                } else {
                    p.skipChildren();
                }
                p.nextToken();
            }
        }, MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    private final Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings;

    GetFieldMappingsResponse(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings) {
        this.mappings = mappings;
    }

    GetFieldMappingsResponse(StreamInput in) throws IOException {
        super(in);
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
        boolean includeTypeName = params.paramAsBoolean(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER,
            DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        builder.startObject();
        for (Map.Entry<String, Map<String, Map<String, FieldMappingMetaData>>> indexEntry : mappings.entrySet()) {
            builder.startObject(indexEntry.getKey());
            builder.startObject(MAPPINGS.getPreferredName());

            if (includeTypeName == false) {
                Map<String, FieldMappingMetaData> mappings = null;
                for (Map.Entry<String, Map<String, FieldMappingMetaData>> typeEntry : indexEntry.getValue().entrySet()) {
                    if (typeEntry.getKey().equals(MapperService.DEFAULT_MAPPING) == false) {
                        assert mappings == null;
                        mappings = typeEntry.getValue();
                    }
                }
                if (mappings != null) {
                    addFieldMappingsToBuilder(builder, params, mappings);
                }
            } else {
                for (Map.Entry<String, Map<String, FieldMappingMetaData>> typeEntry : indexEntry.getValue().entrySet()) {
                    builder.startObject(typeEntry.getKey());
                    addFieldMappingsToBuilder(builder, params, typeEntry.getValue());
                    builder.endObject();
                }
            }

            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void addFieldMappingsToBuilder(XContentBuilder builder,
                                           Params params,
                                           Map<String, FieldMappingMetaData> mappings) throws IOException {
        for (Map.Entry<String, FieldMappingMetaData> fieldEntry : mappings.entrySet()) {
            builder.startObject(fieldEntry.getKey());
            fieldEntry.getValue().toXContent(builder, params);
            builder.endObject();
        }
    }

    public static GetFieldMappingsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        final Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();

                final Map<String, Map<String, FieldMappingMetaData>> typeMappings = PARSER.parse(parser, index);
                mappings.put(index, typeMappings);

                parser.nextToken();
            }
        }

        return new GetFieldMappingsResponse(mappings);
    }

    public static class FieldMappingMetaData implements ToXContentFragment {
        public static final FieldMappingMetaData NULL = new FieldMappingMetaData("", BytesArray.EMPTY);

        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

        private static final ConstructingObjectParser<FieldMappingMetaData, String> PARSER =
            new ConstructingObjectParser<>("field_mapping_meta_data", true,
                a -> new FieldMappingMetaData((String)a[0], (BytesReference)a[1])
            );

        static {
            PARSER.declareField(optionalConstructorArg(),
                (p, c) -> p.text(), FULL_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(optionalConstructorArg(),
                (p, c) -> {
                    final XContentBuilder jsonBuilder = jsonBuilder().copyCurrentStructure(p);
                    final BytesReference bytes = BytesReference.bytes(jsonBuilder);
                    return bytes;
                }, MAPPING, ObjectParser.ValueType.OBJECT);
        }

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

        public static FieldMappingMetaData fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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
    public void writeTo(StreamOutput out) throws IOException {
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
