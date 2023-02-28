/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;

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
        mappings = in.readImmutableMap(StreamInput::readString, mapIn -> {
            if (mapIn.getTransportVersion().before(TransportVersion.V_8_0_0)) {
                int typesSize = mapIn.readVInt();
                assert typesSize == 1 || typesSize == 0 : "Expected 0 or 1 types but got " + typesSize;
                if (typesSize == 0) {
                    return Collections.emptyMap();
                }
                mapIn.readString(); // type
            }
            return mapIn.readImmutableMap(
                StreamInput::readString,
                inpt -> new FieldMappingMetadata(inpt.readString(), inpt.readBytesReference())
            );
        });
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
                if (builder.getRestApiVersion() == RestApiVersion.V_7
                    && params.paramAsBoolean(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY)) {
                    if (indexEntry.getValue().size() > 0) {
                        builder.startObject(MapperService.SINGLE_MAPPING_NAME);
                        addFieldMappingsToBuilder(builder, params, indexEntry.getValue());
                        builder.endObject();
                    }
                } else {
                    addFieldMappingsToBuilder(builder, params, indexEntry.getValue());
                }
            }

            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static void addFieldMappingsToBuilder(XContentBuilder builder, Params params, Map<String, FieldMappingMetadata> mappings)
        throws IOException {
        for (Map.Entry<String, FieldMappingMetadata> fieldEntry : mappings.entrySet()) {
            builder.startObject(fieldEntry.getKey());
            fieldEntry.getValue().toXContent(builder, params);
            builder.endObject();
        }
    }

    public record FieldMappingMetadata(String fullName, BytesReference source) implements ToXContentFragment {

        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

        private static final ConstructingObjectParser<FieldMappingMetadata, String> PARSER = new ConstructingObjectParser<>(
            "field_mapping_meta_data",
            true,
            a -> new FieldMappingMetadata((String) a[0], (BytesReference) a[1])
        );

        /**
         * Returns the mappings as a map. Note that the returned map has a single key which is always the field's {@link Mapper#name}.
         */
        public Map<String, Object> sourceAsMap() {
            return XContentHelper.convertToMap(source, true, XContentType.JSON).v2();
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(mappings, StreamOutput::writeString, (outpt, map) -> {
            if (outpt.getTransportVersion().before(TransportVersion.V_8_0_0)) {
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
