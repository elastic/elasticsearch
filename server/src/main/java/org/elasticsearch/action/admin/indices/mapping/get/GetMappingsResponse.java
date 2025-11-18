/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class GetMappingsResponse extends ActionResponse implements ChunkedToXContentObject {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private final Map<String, MappingMetadata> mappings;

    public GetMappingsResponse(Map<String, MappingMetadata> mappings) {
        this.mappings = mappings;
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public Map<String, MappingMetadata> getMappings() {
        return mappings();
    }

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        MappingMetadata.writeMappingMetadata(out, mappings);
    }

    @Override
    public Iterator<ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(
            Iterators.single((b, p) -> b.startObject()),
            Iterators.map(getMappings().entrySet().iterator(), indexEntry -> (builder, params) -> {
                builder.startObject(indexEntry.getKey());
                if (indexEntry.getValue() != null) {
                    builder.field(MAPPINGS.getPreferredName(), indexEntry.getValue().sourceAsMap());
                } else {
                    builder.startObject(MAPPINGS.getPreferredName()).endObject();
                }
                builder.endObject();
                return builder;
            }),
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
