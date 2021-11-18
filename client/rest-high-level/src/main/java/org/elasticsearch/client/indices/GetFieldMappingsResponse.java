/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/** Response object for {@link GetFieldMappingsRequest} API */
public class GetFieldMappingsResponse {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private static final ObjectParser<Map<String, FieldMappingMetadata>, String> PARSER = new ObjectParser<>(
        MAPPINGS.getPreferredName(),
        true,
        HashMap::new
    );

    static {
        PARSER.declareField((p, fieldMappings, index) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String fieldName = p.currentName();
                final FieldMappingMetadata fieldMappingMetadata = FieldMappingMetadata.fromXContent(p);
                fieldMappings.put(fieldName, fieldMappingMetadata);
                p.nextToken();
            }
        }, MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    private Map<String, Map<String, FieldMappingMetadata>> mappings;

    GetFieldMappingsResponse(Map<String, Map<String, FieldMappingMetadata>> mappings) {
        this.mappings = mappings;
    }

    /**
    * Returns the fields mapping. The return map keys are indexes and fields (as specified in the request).
    */
    public Map<String, Map<String, FieldMappingMetadata>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific index and field.
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

    public static GetFieldMappingsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        final Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();
                final Map<String, FieldMappingMetadata> fieldMappings = PARSER.parse(parser, index);
                mappings.put(index, fieldMappings);
                parser.nextToken();
            }
        }
        return new GetFieldMappingsResponse(mappings);
    }

    public static class FieldMappingMetadata {
        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

        private static final ConstructingObjectParser<FieldMappingMetadata, String> PARSER = new ConstructingObjectParser<>(
            "field_mapping_meta_data",
            true,
            a -> new FieldMappingMetadata((String) a[0], (BytesReference) a[1])
        );

        static {
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.text(), FULL_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(optionalConstructorArg(), (p, c) -> {
                final XContentBuilder jsonBuilder = jsonBuilder().copyCurrentStructure(p);
                final BytesReference bytes = BytesReference.bytes(jsonBuilder);
                return bytes;
            }, MAPPING, ObjectParser.ValueType.OBJECT);
        }

        private String fullName;
        private BytesReference source;

        public FieldMappingMetadata(String fullName, BytesReference source) {
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

        // pkg-private for testing
        BytesReference getSource() {
            return source;
        }

        public static FieldMappingMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public String toString() {
            return "FieldMappingMetadata{fullName='" + fullName + '\'' + ", source=" + source + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof FieldMappingMetadata) == false) return false;
            FieldMappingMetadata that = (FieldMappingMetadata) o;
            return Objects.equals(fullName, that.fullName) && Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fullName, source);
        }
    }

    @Override
    public String toString() {
        return "GetFieldMappingsResponse{" + "mappings=" + mappings + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof GetFieldMappingsResponse) == false) return false;
        GetFieldMappingsResponse that = (GetFieldMappingsResponse) o;
        return Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings);
    }

}
