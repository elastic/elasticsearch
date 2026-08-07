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

    private static final TransportVersion NUMBER_OF_SHARDS_VERSION = TransportVersion.fromName("field_caps_number_of_shards");

    private final String indexName;
    @Nullable
    private final String indexMappingHash;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient TransportVersion originVersion;
    private final IndexMode indexMode;
    private final int numberOfShards;  // 0 indicates that the value is unavailable

    public FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch,
        IndexMode indexMode
    ) {
        this(indexName, indexMappingHash, responseMap, canMatch, indexMode, 0);
    }

    public FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch,
        IndexMode indexMode,
        int numberOfShards
    ) {
        this.indexName = indexName;
        this.indexMappingHash = indexMappingHash;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = TransportVersion.current();
        this.indexMode = indexMode;
        this.numberOfShards = numberOfShards;
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.responseMap = in.readMap(IndexFieldCapabilities::readFrom);
        this.canMatch = in.readBoolean();
        this.originVersion = in.getTransportVersion();
        this.indexMappingHash = in.readOptionalString();
        this.indexMode = IndexMode.readFrom(in);
        this.numberOfShards = in.getTransportVersion().supports(NUMBER_OF_SHARDS_VERSION) ? in.readVInt() : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(responseMap, StreamOutput::writeWriteable);
        out.writeBoolean(canMatch);
        out.writeOptionalString(indexMappingHash);
        IndexMode.writeTo(indexMode, out);
        if (out.getTransportVersion().supports(NUMBER_OF_SHARDS_VERSION)) {
            out.writeVInt(numberOfShards);
        }
    }

    private record CompressedGroup(String[] indices, int[] numberOfShardsPerIndex, IndexMode indexMode, String mappingHash, int[] fields) {}

    static List<FieldCapabilitiesIndexResponse> readList(StreamInput input) throws IOException {
        final int ungrouped = input.readVInt();
        final ArrayList<FieldCapabilitiesIndexResponse> responses = new ArrayList<>(ungrouped);
        for (int i = 0; i < ungrouped; i++) {
            responses.add(new FieldCapabilitiesIndexResponse(input));
        }
        final int groups = input.readVInt();
        collectCompressedResponses(input, groups, responses);
        return responses;
    }

    private static void collectCompressedResponses(StreamInput input, int groups, ArrayList<FieldCapabilitiesIndexResponse> responses)
        throws IOException {
        final CompressedGroup[] compressedGroups = new CompressedGroup[groups];
        for (int i = 0; i < groups; i++) {
            final String[] indices = input.readStringArray();
            final int[] numberOfShardsPerIndex = (input.getTransportVersion().supports(NUMBER_OF_SHARDS_VERSION))
                ? input.readIntArray()
                : new int[indices.length];
            final IndexMode indexMode = IndexMode.readFrom(input);
            final String mappingHash = input.readString();
            compressedGroups[i] = new CompressedGroup(indices, numberOfShardsPerIndex, indexMode, mappingHash, input.readIntArray());
        }

        final IndexFieldCapabilities[] ifcLookup = input.readArray(IndexFieldCapabilities::readFrom, IndexFieldCapabilities[]::new);
        for (CompressedGroup compressedGroup : compressedGroups) {
            final Map<String, IndexFieldCapabilities> ifc = Maps.newMapWithExpectedSize(compressedGroup.fields.length);
            for (int i : compressedGroup.fields) {
                var val = ifcLookup[i];
                ifc.put(val.name(), val);
            }
            for (int j = 0; j < compressedGroup.indices.length; j++) {
                responses.add(
                    new FieldCapabilitiesIndexResponse(
                        compressedGroup.indices[j],
                        compressedGroup.mappingHash,
                        ifc,
                        true,
                        compressedGroup.indexMode,
                        compressedGroup.numberOfShardsPerIndex[j]
                    )
                );
            }
        }
    }

    static void writeList(StreamOutput output, List<FieldCapabilitiesIndexResponse> responses) throws IOException {
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
        writeCompressedResponses(output, groupedResponsesMap);
    }

    private static void writeCompressedResponses(StreamOutput output, Map<String, List<FieldCapabilitiesIndexResponse>> groupedResponsesMap)
        throws IOException {
        final Map<IndexFieldCapabilities, Integer> fieldDedupMap = new LinkedHashMap<>();
        output.writeCollection(groupedResponsesMap.values(), (o, fieldCapabilitiesIndexResponses) -> {
            o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeString(r.indexName));
            if (output.getTransportVersion().supports(NUMBER_OF_SHARDS_VERSION)) {
                o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeInt(r.numberOfShards));
            }
            var first = fieldCapabilitiesIndexResponses.get(0);
            IndexMode.writeTo(first.indexMode, o);
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

    /**
     * Returns the total number of primary shards configured for this index, or {@code 0} if the
     * value is not available (e.g. when the response came from a node that pre-dates
     * {@link #NUMBER_OF_SHARDS_VERSION}).
     */
    public int getNumberOfShards() {
        return numberOfShards;
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
            && numberOfShards == that.numberOfShards
            && Objects.equals(indexName, that.indexName)
            && Objects.equals(indexMappingHash, that.indexMappingHash)
            && Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexMappingHash, responseMap, canMatch, numberOfShards);
    }
}
