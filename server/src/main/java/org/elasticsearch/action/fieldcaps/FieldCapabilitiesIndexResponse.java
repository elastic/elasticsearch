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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class FieldCapabilitiesIndexResponse implements Writeable {

    private static final TransportVersion NUMBER_OF_SHARDS_VERSION = TransportVersion.fromName("field_caps_number_of_shards");
    public static final TransportVersion FIELD_CAPS_INDEX_VERSIONS = TransportVersion.fromName("field_caps_index_versions");
    // Shares the shuffle branch's unreleased transport version: the generator assigns one new id per branch.
    public static final TransportVersion SHARD_DOC_STATS_VERSION = TransportVersion.fromName("esql_shuffle_stage_execution");

    private final String indexName;
    @Nullable
    private final String indexMappingHash;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient TransportVersion originVersion;
    private final IndexMode indexMode;
    private final int numberOfShards;  // 0 indicates that the value is unavailable
    private final long indexSettingsVersion;
    private final long mappingVersion;
    private final long shardDocCount;  // -1 indicates that the value is unavailable
    private final long shardSizeInBytes;  // -1 indicates that the value is unavailable
    private final Map<String, IndexFieldStats> fieldStats;

    public FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch,
        IndexMode indexMode,
        int numberOfShards,
        long indexSettingsVersion,
        long mappingVersion
    ) {
        this(
            indexName,
            indexMappingHash,
            responseMap,
            canMatch,
            indexMode,
            numberOfShards,
            indexSettingsVersion,
            mappingVersion,
            -1,
            -1,
            Map.of()
        );
    }

    public FieldCapabilitiesIndexResponse(
        String indexName,
        @Nullable String indexMappingHash,
        Map<String, IndexFieldCapabilities> responseMap,
        boolean canMatch,
        IndexMode indexMode,
        int numberOfShards,
        long indexSettingsVersion,
        long mappingVersion,
        long shardDocCount,
        long shardSizeInBytes,
        Map<String, IndexFieldStats> fieldStats
    ) {
        this.indexName = indexName;
        this.indexMappingHash = indexMappingHash;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = TransportVersion.current();
        this.indexMode = indexMode;
        this.numberOfShards = numberOfShards;
        this.indexSettingsVersion = indexSettingsVersion;
        this.mappingVersion = mappingVersion;
        this.shardDocCount = shardDocCount;
        this.shardSizeInBytes = shardSizeInBytes;
        this.fieldStats = Objects.requireNonNull(fieldStats);
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.responseMap = in.readMap(IndexFieldCapabilities::readFrom);
        this.canMatch = in.readBoolean();
        this.originVersion = in.getTransportVersion();
        this.indexMappingHash = in.readOptionalString();
        this.indexMode = IndexMode.readFrom(in);
        this.numberOfShards = in.getTransportVersion().supports(NUMBER_OF_SHARDS_VERSION) ? in.readVInt() : 0;
        if (in.getTransportVersion().supports(FIELD_CAPS_INDEX_VERSIONS)) {
            this.indexSettingsVersion = in.readVLong();
            this.mappingVersion = in.readVLong();
        } else {
            this.indexSettingsVersion = 0;
            this.mappingVersion = 0;
        }
        if (in.getTransportVersion().supports(SHARD_DOC_STATS_VERSION)) {
            this.shardDocCount = in.readZLong();
            this.shardSizeInBytes = in.readZLong();
            this.fieldStats = in.readImmutableMap(IndexFieldStats::new);
        } else {
            this.shardDocCount = -1;
            this.shardSizeInBytes = -1;
            this.fieldStats = Map.of();
        }
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
        if (out.getTransportVersion().supports(FIELD_CAPS_INDEX_VERSIONS)) {
            out.writeVLong(indexSettingsVersion);
            out.writeVLong(mappingVersion);
        }
        if (out.getTransportVersion().supports(SHARD_DOC_STATS_VERSION)) {
            out.writeZLong(shardDocCount);
            out.writeZLong(shardSizeInBytes);
            out.writeMap(fieldStats, StreamOutput::writeWriteable);
        }
    }

    private record CompressedGroup(
        String[] indices,
        int[] numberOfShardsPerIndex,
        long[] settingsVersionPerIndex,
        long[] mappingVersionPerIndex,
        long[] shardDocCountPerIndex,
        long[] shardSizeInBytesPerIndex,
        List<Map<String, IndexFieldStats>> fieldStatsPerIndex,
        IndexMode indexMode,
        String mappingHash,
        int[] fields
    ) {}

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
            final long[] settingsVersionPerIndex;
            final long[] mappingVersionPerIndex;
            if (input.getTransportVersion().supports(FIELD_CAPS_INDEX_VERSIONS)) {
                settingsVersionPerIndex = input.readVLongArray();
                mappingVersionPerIndex = input.readVLongArray();
            } else {
                settingsVersionPerIndex = new long[indices.length];
                mappingVersionPerIndex = new long[indices.length];
            }
            final long[] shardDocCountPerIndex;
            final long[] shardSizeInBytesPerIndex;
            final List<Map<String, IndexFieldStats>> fieldStatsPerIndex;
            if (input.getTransportVersion().supports(SHARD_DOC_STATS_VERSION)) {
                shardDocCountPerIndex = input.readLongArray();
                shardSizeInBytesPerIndex = input.readLongArray();
                fieldStatsPerIndex = input.readCollectionAsList(in -> in.readImmutableMap(IndexFieldStats::new));
            } else {
                fieldStatsPerIndex = Collections.nCopies(indices.length, Map.of());
                shardDocCountPerIndex = new long[indices.length];
                shardSizeInBytesPerIndex = new long[indices.length];
                Arrays.fill(shardDocCountPerIndex, -1);
                Arrays.fill(shardSizeInBytesPerIndex, -1);
            }
            final IndexMode indexMode = IndexMode.readFrom(input);
            final String mappingHash = input.readString();
            compressedGroups[i] = new CompressedGroup(
                indices,
                numberOfShardsPerIndex,
                settingsVersionPerIndex,
                mappingVersionPerIndex,
                shardDocCountPerIndex,
                shardSizeInBytesPerIndex,
                fieldStatsPerIndex,
                indexMode,
                mappingHash,
                input.readIntArray()
            );
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
                        compressedGroup.numberOfShardsPerIndex[j],
                        compressedGroup.settingsVersionPerIndex[j],
                        compressedGroup.mappingVersionPerIndex[j],
                        compressedGroup.shardDocCountPerIndex[j],
                        compressedGroup.shardSizeInBytesPerIndex[j],
                        compressedGroup.fieldStatsPerIndex.get(j)
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
            if (output.getTransportVersion().supports(FIELD_CAPS_INDEX_VERSIONS)) {
                o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeVLong(r.indexSettingsVersion));
                o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeVLong(r.mappingVersion));
            }
            if (output.getTransportVersion().supports(SHARD_DOC_STATS_VERSION)) {
                o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeLong(r.shardDocCount));
                o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeLong(r.shardSizeInBytes));
                o.writeCollection(fieldCapabilitiesIndexResponses, (oo, r) -> oo.writeMap(r.fieldStats, StreamOutput::writeWriteable));
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

    /**
     * Index setting version or {@code 0} if unavailable.
     */
    public long getIndexSettingsVersion() {
        return indexSettingsVersion;
    }

    /**
     * Mapping version or {@code 0} if unavailable.
     */
    public long getMappingVersion() {
        return mappingVersion;
    }

    /**
     * Live document count of the one shard copy that answered for this index, or {@code -1} if unavailable. Field caps
     * reads a single shard per index, so this is a sample: multiply by {@link #getNumberOfShards()} to estimate the index.
     */
    public long getShardDocCount() {
        return shardDocCount;
    }

    /**
     * Segment size in bytes of the one shard copy that answered for this index, or {@code -1} if unavailable.
     * See {@link #getShardDocCount()} for how to extrapolate it to the index.
     */
    public long getShardSizeInBytes() {
        return shardSizeInBytes;
    }

    /**
     * Per-field stats of the one shard copy that answered for this index, keyed by field name. Empty unless the
     * request set {@link FieldCapabilitiesRequest#includeFieldStats()}; fields without cheap stats are left out.
     */
    public Map<String, IndexFieldStats> getFieldStats() {
        return fieldStats;
    }

    /**
     * Copy of this response under another index name, keeping everything else, including the shard stats.
     */
    FieldCapabilitiesIndexResponse withIndexName(String newIndexName) {
        return withIndexNameAndFields(newIndexName, indexMappingHash, responseMap, canMatch);
    }

    /**
     * Copy of this response with other field capabilities, keeping the shard stats.
     */
    FieldCapabilitiesIndexResponse withIndexNameAndFields(
        String newIndexName,
        @Nullable String newIndexMappingHash,
        Map<String, IndexFieldCapabilities> newResponseMap,
        boolean newCanMatch
    ) {
        return new FieldCapabilitiesIndexResponse(
            newIndexName,
            newIndexMappingHash,
            newResponseMap,
            newCanMatch,
            indexMode,
            numberOfShards,
            indexSettingsVersion,
            mappingVersion,
            shardDocCount,
            shardSizeInBytes,
            fieldStats
        );
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
            && indexSettingsVersion == that.indexSettingsVersion
            && mappingVersion == that.mappingVersion
            && shardDocCount == that.shardDocCount
            && shardSizeInBytes == that.shardSizeInBytes
            && fieldStats.equals(that.fieldStats)
            && Objects.equals(indexName, that.indexName)
            && Objects.equals(indexMappingHash, that.indexMappingHash)
            && Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            indexName,
            indexMappingHash,
            responseMap,
            canMatch,
            numberOfShards,
            indexSettingsVersion,
            mappingVersion,
            shardDocCount,
            shardSizeInBytes,
            fieldStats
        );
    }
}
