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

package org.elasticsearch.client.indices;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/** Response object for {@link GetFieldMappingsRequest} API */
public class GetFieldMappingsResponse {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private static final ObjectParser<Map<String, FieldMappingMetaData>, String> PARSER =
        new ObjectParser<>(MAPPINGS.getPreferredName(), true, HashMap::new);

    static {
        PARSER.declareField((p, fieldMappings, index) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String fieldName = p.currentName();
                final FieldMappingMetaData fieldMappingMetaData = FieldMappingMetaData.fromXContent(p);
                fieldMappings.put(fieldName, fieldMappingMetaData);
                p.nextToken();
            }
        }, MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    private Map<String, Map<String, FieldMappingMetaData>> mappings;

    GetFieldMappingsResponse(Map<String, Map<String, FieldMappingMetaData>> mappings) {
        this.mappings = mappings;
    }


     /**
     * Returns the fields mapping. The return map keys are indexes and fields (as specified in the request).
     */
    public Map<String, Map<String, FieldMappingMetaData>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific index and field.
     *
     * @param field field name as specified in the {@link GetFieldMappingsRequest}
     * @return FieldMappingMetaData for the requested field or null if not found.
     */
    public FieldMappingMetaData fieldMappings(String index, String field) {
        Map<String, FieldMappingMetaData> indexMapping = mappings.get(index);
        if (indexMapping == null) {
            return null;
        }
        return indexMapping.get(field);
    }


    public static GetFieldMappingsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        final Map<String, Map<String, FieldMappingMetaData>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();
                final Map<String, FieldMappingMetaData> fieldMappings = PARSER.parse(parser, index);
                mappings.put(index, fieldMappings);
                parser.nextToken();
            }
        }
        return new GetFieldMappingsResponse(mappings);
    }

    public static class FieldMappingMetaData {
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

        /**
         * Returns the mappings as a map. Note that the returned map has a single key which is always the field's {@link Mapper#name}.
         */
        public Map<String, Object> sourceAsMap() {
            return XContentHelper.convertToMap(source, true, XContentType.JSON).v2();
        }

        //pkg-private for testing
        BytesReference getSource() {
            return source;
        }

        public static FieldMappingMetaData fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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
            return Objects.equals(fullName, that.fullName) && Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fullName, source);
        }
    }


    @Override
    public String toString() {
        return "GetFieldMappingsResponse{" +  "mappings=" + mappings + '}';
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
