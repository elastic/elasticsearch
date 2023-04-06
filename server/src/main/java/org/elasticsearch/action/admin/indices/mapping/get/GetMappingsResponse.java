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
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

public class GetMappingsResponse extends ActionResponse implements ChunkedToXContentObject {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private final Map<String, MappingMetadata> mappings;

    public GetMappingsResponse(Map<String, MappingMetadata> mappings) {
        this.mappings = mappings;
    }

    GetMappingsResponse(StreamInput in) throws IOException {
        super(in);
        mappings = in.readImmutableMap(StreamInput::readString, in.getTransportVersion().before(TransportVersion.V_8_0_0) ? i -> {
            int mappingCount = i.readVInt();
            assert mappingCount == 1 || mappingCount == 0 : "Expected 0 or 1 mappings but got " + mappingCount;
            if (mappingCount == 1) {
                String type = i.readString();
                assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected type [_doc] but got [" + type + "]";
                return new MappingMetadata(i);
            } else {
                return MappingMetadata.EMPTY_MAPPINGS;
            }
        } : i -> i.readBoolean() ? new MappingMetadata(i) : MappingMetadata.EMPTY_MAPPINGS);
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public Map<String, MappingMetadata> getMappings() {
        return mappings();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        MappingMetadata.writeMappingMetadata(out, mappings);
    }

    @Override
    public Iterator<ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(
            Iterators.single((b, p) -> b.startObject()),
            getMappings().entrySet().stream().map(indexEntry -> (ToXContent) (builder, params) -> {
                builder.startObject(indexEntry.getKey());
                boolean includeTypeName = params.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
                if (builder.getRestApiVersion() == RestApiVersion.V_7 && includeTypeName && indexEntry.getValue() != null) {
                    builder.startObject(MAPPINGS.getPreferredName());

                    if (indexEntry.getValue() != MappingMetadata.EMPTY_MAPPINGS) {
                        builder.field(MapperService.SINGLE_MAPPING_NAME, indexEntry.getValue().sourceAsMap());
                    }
                    builder.endObject();

                } else if (indexEntry.getValue() != null) {
                    builder.field(MAPPINGS.getPreferredName(), indexEntry.getValue().sourceAsMap());
                } else {
                    builder.startObject(MAPPINGS.getPreferredName()).endObject();
                }
                builder.endObject();
                return builder;
            }).iterator(),
            Iterators.single((b, p) -> b.endObject())
        );
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
