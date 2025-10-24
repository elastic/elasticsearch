/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class FieldCapabilitiesIndexResponse implements Writeable {
    private static final TransportVersion MAPPING_HASH_VERSION = TransportVersions.V_8_2_0;

    private final String indexName;
    @Nullable
    private final String indexMappingHash;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient TransportVersion originVersion;
    private final IndexMode indexMode;

    public FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch,
        IndexMode indexMode
    ) {
        this.indexName = indexName;
        this.indexMappingHash = indexMappingHash;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = TransportVersion.current();
        this.indexMode = indexMode;
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
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            this.indexMode = IndexMode.readFrom(in);
        } else {
            this.indexMode = IndexMode.STANDARD;
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            IndexMode.writeTo(indexMode, out);
        }
    }

    private record CompressedGroup(String[] indices, IndexMode indexMode, String mappingHash, int[] fields) {}

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
        if (input.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            collectCompressedResponses(input, groups, responses);
        } else {
            collectResponsesLegacyFormat(input, groups, responses);
        }
        return responses;
    }

    private static void collectCompressedResponses(StreamInput input, int groups, ArrayList<FieldCapabilitiesIndexResponse> responses)
        throws IOException {
        final CompressedGroup[] compressedGroups = new CompressedGroup[groups];
        final boolean readIndexMode = input.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0);
        for (int i = 0; i < groups; i++) {
            final String[] indices = input.readStringArray();
            final IndexMode indexMode = readIndexMode ? IndexMode.readFrom(input) : IndexMode.STANDARD;
            final String mappingHash = input.readString();
            compressedGroups[i] = new CompressedGroup(indices, indexMode, mappingHash, input.readIntArray());
        }
        final IndexFieldCapabilities[] ifcLookup = input.readArray(IndexFieldCapabilities::readFrom, IndexFieldCapabilities[]::new);
        for (CompressedGroup compressedGroup : compressedGroups) {
            final Map<String, IndexFieldCapabilities> ifc = Maps.newMapWithExpectedSize(compressedGroup.fields.length);
            for (int i : compressedGroup.fields) {
                var val = ifcLookup[i];
                ifc.put(val.name(), val);
            }
            for (String index : compressedGroup.indices) {
                responses.add(new FieldCapabilitiesIndexResponse(index, compressedGroup.mappingHash, ifc, true, compressedGroup.indexMode));
            }
        }
    }

    private static void collectResponsesLegacyFormat(StreamInput input, int groups, ArrayList<FieldCapabilitiesIndexResponse> responses)
        throws IOException {
        for (int i = 0; i < groups; i++) {
            final List<String> indices = input.readStringCollectionAsList();
            final String mappingHash = input.readString();
            final Map<String, IndexFieldCapabilities> ifc = input.readMap(IndexFieldCapabilities::readFrom);
            for (String index : indices) {
                responses.add(new FieldCapabilitiesIndexResponse(index, mappingHash, ifc, true, IndexMode.STANDARD));
            }
        }
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
        if (output.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            writeCompressedResponses(output, groupedResponsesMap);
        } else {
            writeResponsesLegacyFormat(output, groupedResponsesMap);
        }
    }

    private static void writeResponsesLegacyFormat(
        StreamOutput output,
        Map<String, List<FieldCapabilitiesIndexResponse>> groupedResponsesMap
    ) throws IOException {
        output.writeCollection(groupedResponsesMap.values(), (o, fieldCapabilitiesIndexResponses) -> {
            o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeString(r.indexName));
            var first = fieldCapabilitiesIndexResponses.get(0);
            o.writeString(first.indexMappingHash);
            o.writeMap(first.responseMap, StreamOutput::writeWriteable);
        });
    }

    private static void writeCompressedResponses(StreamOutput output, Map<String, List<FieldCapabilitiesIndexResponse>> groupedResponsesMap)
        throws IOException {
        final Map<IndexFieldCapabilities, Integer> fieldDedupMap = new LinkedHashMap<>();
        output.writeCollection(groupedResponsesMap.values(), (o, fieldCapabilitiesIndexResponses) -> {
            o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeString(r.indexName));
            var first = fieldCapabilitiesIndexResponses.get(0);
            if (output.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                IndexMode.writeTo(first.indexMode, o);
            }
            o.writeString(first.indexMappingHash);
            o.writeVInt(first.responseMap.size());
            for (IndexFieldCapabilities ifc : first.responseMap.values()) {
                Integer offset = fieldDedupMap.size();
                final Integer found = fieldDedupMap.putIfAbsent(ifc, offset);
                o.writeInt(found == null ? offset : found);
            }
        });
        // this is a linked hash map so the key-set is written in insertion order, so we can just write it out in order and then read it
        // back as an array of FieldCapabilitiesIndexResponse in #collectCompressedResponses to use as a lookup
        output.writeCollection(fieldDedupMap.keySet());
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

    public IndexMode getIndexMode() {
        return indexMode;
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
