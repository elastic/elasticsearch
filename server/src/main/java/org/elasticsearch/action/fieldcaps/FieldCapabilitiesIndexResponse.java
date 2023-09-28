/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class FieldCapabilitiesIndexResponse implements Writeable {
    private static final TransportVersion MAPPING_HASH_VERSION = TransportVersions.V_8_2_0;

    private final String indexName;
    @Nullable
    private final String indexMappingHash;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient TransportVersion originVersion;

    FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch
    ) {
        this.indexName = indexName;
        this.indexMappingHash = indexMappingHash;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = TransportVersion.current();
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.responseMap = in.readMap(IndexFieldCapabilities::readFrom);
        this.canMatch = in.readBoolean();
        this.originVersion = in.getTransportVersion();
        if (in.getTransportVersion().onOrAfter(MAPPING_HASH_VERSION)) {
            this.indexMappingHash = in.readOptionalString();
        } else {
            this.indexMappingHash = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(responseMap, StreamOutput::writeWriteable);
        out.writeBoolean(canMatch);
        if (out.getTransportVersion().onOrAfter(MAPPING_HASH_VERSION)) {
            out.writeOptionalString(indexMappingHash);
        }
    }

    static List<FieldCapabilitiesIndexResponse> readList(StreamInput input) throws IOException {
        if (input.getTransportVersion().before(MAPPING_HASH_VERSION)) {
            return input.readCollectionAsList(FieldCapabilitiesIndexResponse::new);
        }
        final int ungrouped = input.readVInt();
        final ArrayList<FieldCapabilitiesIndexResponse> responses = new ArrayList<>(ungrouped);
        for (int i = 0; i < ungrouped; i++) {
            responses.add(new FieldCapabilitiesIndexResponse(input));
        }
        final int groups = input.readVInt();
        for (int i = 0; i < groups; i++) {
            final List<String> indices = input.readStringCollectionAsList();
            final String mappingHash = input.readString();
            final Map<String, IndexFieldCapabilities> ifc = input.readMap(IndexFieldCapabilities::readFrom);
            for (String index : indices) {
                responses.add(new FieldCapabilitiesIndexResponse(index, mappingHash, ifc, true));
            }
        }
        return responses;
    }

    static void writeList(StreamOutput output, List<FieldCapabilitiesIndexResponse> responses) throws IOException {
        if (output.getTransportVersion().before(MAPPING_HASH_VERSION)) {
            output.writeCollection(responses);
            return;
        }

        Map<String, List<FieldCapabilitiesIndexResponse>> groupedResponsesMap = new HashMap<>();
        final List<FieldCapabilitiesIndexResponse> ungroupedResponses = new ArrayList<>();
        for (FieldCapabilitiesIndexResponse r : responses) {
            if (r.canMatch && r.indexMappingHash != null) {
                groupedResponsesMap.computeIfAbsent(r.indexMappingHash, k -> new ArrayList<>()).add(r);
            } else {
                ungroupedResponses.add(r);
            }
        }
        output.writeCollection(ungroupedResponses);
        output.writeCollection(groupedResponsesMap.values(), (o, fieldCapabilitiesIndexResponses) -> {
            o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeString(r.indexName));
            var first = fieldCapabilitiesIndexResponses.get(0);
            o.writeString(first.indexMappingHash);
            o.writeMap(first.responseMap, StreamOutput::writeWriteable);
        });
    }

    /**
     * Get the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the index mapping hash associated with this index if exists
     */
    @Nullable
    public String getIndexMappingHash() {
        return indexMappingHash;
    }

    public boolean canMatch() {
        return canMatch;
    }

    /**
     * Get the field capabilities map
     */
    public Map<String, IndexFieldCapabilities> get() {
        return responseMap;
    }

    /**
     *
     * Get the field capabilities for the provided {@code field}
     */
    public IndexFieldCapabilities getField(String field) {
        return responseMap.get(field);
    }

    TransportVersion getOriginVersion() {
        return originVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesIndexResponse that = (FieldCapabilitiesIndexResponse) o;
        return canMatch == that.canMatch
            && Objects.equals(indexName, that.indexName)
            && Objects.equals(indexMappingHash, that.indexMappingHash)
            && Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexMappingHash, responseMap, canMatch);
    }
}
