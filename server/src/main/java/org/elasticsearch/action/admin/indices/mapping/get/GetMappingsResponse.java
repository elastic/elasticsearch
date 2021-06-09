/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

public class GetMappingsResponse extends ActionResponse implements ToXContentFragment {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private final ImmutableOpenMap<String, MappingMetadata> mappings;

    public GetMappingsResponse(ImmutableOpenMap<String, MappingMetadata> mappings) {
        this.mappings = mappings;
    }

    GetMappingsResponse(StreamInput in) throws IOException {
        super(in);
        mappings = in.readImmutableMap(StreamInput::readString, in.getVersion().before(Version.V_8_0_0) ? i -> {
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

    public ImmutableOpenMap<String, MappingMetadata> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, MappingMetadata> getMappings() {
        return mappings();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        MappingMetadata.writeMappingMetadata(out, mappings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (final ObjectObjectCursor<String, MappingMetadata> indexEntry : getMappings()) {
            builder.startObject(indexEntry.key);
            boolean includeTypeName = params.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER,
                DEFAULT_INCLUDE_TYPE_NAME_POLICY);
            if (builder.getRestApiVersion() == RestApiVersion.V_7 && includeTypeName && indexEntry.value != null) {
                builder.startObject(MAPPINGS.getPreferredName());

                if (indexEntry.value != MappingMetadata.EMPTY_MAPPINGS) {
                    builder.field(MapperService.SINGLE_MAPPING_NAME, indexEntry.value.sourceAsMap());
                }
                builder.endObject();

            } else if (indexEntry.value != null) {
                builder.field(MAPPINGS.getPreferredName(), indexEntry.value.sourceAsMap());
            } else {
                builder.startObject(MAPPINGS.getPreferredName()).endObject();
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
