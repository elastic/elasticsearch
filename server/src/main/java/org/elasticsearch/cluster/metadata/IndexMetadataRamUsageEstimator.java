/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;

import java.util.List;
import java.util.Map;

/**
 * Heap footprint estimation helpers for {@link IndexMetadata}.
 */
final class IndexMetadataRamUsageEstimator {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IndexMetadata.class);
    // Settings stores values in a TreeMap; these constants capture entry overhead without needing the map reference.
    private static final long SETTINGS_BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Settings.class);
    private static final long SETTINGS_TREE_MAP_BYTES = RamUsageEstimator.shallowSizeOfInstance(java.util.TreeMap.class);
    // TreeMap.Entry: header + 5 refs (key, value, parent, left, right) + 1 byte (color flag)
    private static final long SETTINGS_TREE_MAP_ENTRY_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 1
    );
    private static final long COMPRESSED_XCONTENT_BASE = RamUsageEstimator.shallowSizeOfInstance(CompressedXContent.class);
    private static final long MAPPING_METADATA_BASE = RamUsageEstimator.shallowSizeOfInstance(MappingMetadata.class);
    private static final long ALIAS_METADATA_BASE = RamUsageEstimator.shallowSizeOfInstance(AliasMetadata.class);
    private static final long ROLLOVER_INFO_BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RolloverInfo.class);

    private IndexMetadataRamUsageEstimator() {}

    static long estimate(IndexMetadata indexMetadata) {
        long size = BASE_RAM_BYTES_USED;
        size += sizeOfIndex(indexMetadata.getIndex());
        size += estimateSettingsHeap(indexMetadata.getSettings());
        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping != null) {
            size += estimateMappingMetadataHeap(mapping);
        }
        size += RamUsageEstimator.sizeOf(indexMetadata.getPrimaryTerms());
        size += RamUsageEstimator.sizeOfMap(indexMetadata.getInSyncAllocationIds());
        size += estimateAliasesHeap(indexMetadata.getAliases());
        size += RamUsageEstimator.sizeOfMap(indexMetadata.getCustomData());
        size += estimateInferenceFieldsHeap(indexMetadata.getInferenceFields());
        size += estimateRolloverInfosHeap(indexMetadata.getRolloverInfos());
        size += sizeOfTransportVersion(indexMetadata.getTransportVersion());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getState());
        size += RamUsageEstimator.sizeOfCollection(indexMetadata.getRoutingPaths());
        size += RamUsageEstimator.sizeOfCollection(indexMetadata.getTimeSeriesDimensions());
        size += estimateDiscoveryNodeFiltersHeap(indexMetadata.requireFilters());
        size += estimateDiscoveryNodeFiltersHeap(indexMetadata.includeFilters());
        size += estimateDiscoveryNodeFiltersHeap(indexMetadata.excludeFilters());
        size += estimateDiscoveryNodeFiltersHeap(indexMetadata.getInitialRecoveryFilters());
        size += sizeOfIndexVersion(indexMetadata.getCreationVersion());
        size += sizeOfIndexVersion(indexMetadata.getMappingsUpdatedVersion());
        size += sizeOfIndexVersion(indexMetadata.getCompatibilityVersion());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getWaitForActiveShards());
        size += estimateIndexLongFieldRangeHeap(indexMetadata.getTimestampRange());
        size += estimateIndexLongFieldRangeHeap(indexMetadata.getEventIngestedRange());
        size += RamUsageEstimator.sizeOfCollection(indexMetadata.getTierPreference());
        size += RamUsageEstimator.sizeOf(indexMetadata.getLifecyclePolicyName());
        size += sizeOfLifecycleExecutionState(indexMetadata.getLifecycleExecutionState());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getAutoExpandReplicas());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getIndexMode());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getTimeSeriesStart());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getTimeSeriesEnd());
        size += estimateIndexMetadataStatsHeap(indexMetadata.getStats());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getWriteLoadForecast());
        size += RamUsageEstimator.shallowSizeOf(indexMetadata.getShardSizeInBytesForecast());
        size += estimateIndexReshardingMetadataHeap(indexMetadata.getReshardingMetadata());
        return size;
    }

    /**
     * Estimates the heap footprint of a {@link MappingMetadata} instance using its public API.
     * Exposed for callers (e.g. {@code StatelessMemoryMetricsService}) that need to subtract
     * duplicate mapping costs when summing across indices that share a mapping instance.
     */
    static long estimateMappingMetadataHeap(MappingMetadata mapping) {
        return MAPPING_METADATA_BASE + RamUsageEstimator.sizeOf(mapping.type()) + estimateCompressedXContentHeap(mapping.source());
    }

    private static long sizeOfIndex(Index index) {
        return RamUsageEstimator.shallowSizeOf(index) + RamUsageEstimator.sizeOf(index.getName()) + RamUsageEstimator.sizeOf(
            index.getUUID()
        );
    }

    private static long estimateSettingsHeap(Settings settings) {
        if (settings.isEmpty()) {
            return SETTINGS_BASE_RAM_BYTES_USED;
        }
        // Keys and string values are interned via Settings.internKeyOrValue — not counted to avoid 4x over-counting.
        // List-value overhead is minor and omitted for simplicity.
        // secureSettings omitted: never populated on index-level Settings.
        return SETTINGS_BASE_RAM_BYTES_USED + SETTINGS_TREE_MAP_BYTES + (long) settings.size() * SETTINGS_TREE_MAP_ENTRY_BYTES;
    }

    private static long estimateCompressedXContentHeap(CompressedXContent content) {
        return COMPRESSED_XCONTENT_BASE + RamUsageEstimator.sizeOf(content.compressed()) + RamUsageEstimator.sizeOf(content.getSha256());
    }

    private static long estimateAliasesHeap(Map<String, AliasMetadata> aliases) {
        long size = RamUsageEstimator.shallowSizeOf(aliases);
        long sizeOfEntry = -1;
        for (Map.Entry<String, AliasMetadata> entry : aliases.entrySet()) {
            if (sizeOfEntry == -1) {
                sizeOfEntry = RamUsageEstimator.shallowSizeOf(entry);
            }
            size += sizeOfEntry;
            size += RamUsageEstimator.sizeOf(entry.getKey());
            size += estimateAliasMetadataHeap(entry.getValue());
        }
        return RamUsageEstimator.alignObjectSize(size);
    }

    private static long estimateAliasMetadataHeap(AliasMetadata alias) {
        long size = ALIAS_METADATA_BASE;
        size += RamUsageEstimator.sizeOf(alias.getAlias());
        CompressedXContent filter = alias.filter();
        if (filter != null) {
            size += estimateCompressedXContentHeap(filter);
        }
        size += RamUsageEstimator.sizeOf(alias.indexRouting());
        size += RamUsageEstimator.sizeOf(alias.searchRouting());
        size += RamUsageEstimator.sizeOfCollection(alias.searchRoutingValues());
        size += RamUsageEstimator.shallowSizeOf(alias.writeIndex());
        size += RamUsageEstimator.shallowSizeOf(alias.isHidden());
        return size;
    }

    private static long estimateRolloverInfosHeap(Map<String, RolloverInfo> rolloverInfos) {
        long size = RamUsageEstimator.shallowSizeOf(rolloverInfos);
        long sizeOfEntry = -1;
        for (Map.Entry<String, RolloverInfo> entry : rolloverInfos.entrySet()) {
            if (sizeOfEntry == -1) {
                sizeOfEntry = RamUsageEstimator.shallowSizeOf(entry);
            }
            size += sizeOfEntry;
            size += RamUsageEstimator.sizeOf(entry.getKey());
            size += estimateRolloverInfoHeap(entry.getValue());
        }
        return RamUsageEstimator.alignObjectSize(size);
    }

    private static long estimateRolloverInfoHeap(RolloverInfo rolloverInfo) {
        long size = ROLLOVER_INFO_BASE_RAM_BYTES_USED;
        size += RamUsageEstimator.sizeOf(rolloverInfo.getAlias());
        size += estimateMetConditionsHeap(rolloverInfo.getMetConditions());
        return size;
    }

    private static long estimateMetConditionsHeap(List<Condition<?>> metConditions) {
        long size = RamUsageEstimator.shallowSizeOf(metConditions);
        if (metConditions.isEmpty()) {
            return size;
        }
        size += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + metConditions.size() * (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        for (Condition<?> condition : metConditions) {
            size += estimateConditionHeap(condition);
        }
        return RamUsageEstimator.alignObjectSize(size);
    }

    private static long estimateConditionHeap(Condition<?> condition) {
        long size = RamUsageEstimator.shallowSizeOf(condition);
        size += RamUsageEstimator.sizeOf(condition.name());
        size += RamUsageEstimator.shallowSizeOf(condition.type());
        size += estimateConditionValueHeap(condition.value());
        return size;
    }

    private static long estimateConditionValueHeap(Object value) {
        if (value == null) {
            return 0L;
        }
        return switch (value) {
            case Long l -> RamUsageEstimator.sizeOf(l);
            case Integer i -> RamUsageEstimator.sizeOf(i);
            case TimeValue timeValue -> RamUsageEstimator.shallowSizeOf(timeValue);
            case ByteSizeValue byteSizeValue -> RamUsageEstimator.shallowSizeOf(byteSizeValue);
            default -> throw new IllegalStateException("unexpected rollover condition value type [" + value.getClass() + "]");
        };
    }

    private static long sizeOfTransportVersion(@Nullable TransportVersion version) {
        if (version == null) {
            return 0L;
        }
        long size = RamUsageEstimator.shallowSizeOf(version);
        size += RamUsageEstimator.sizeOf(version.name());
        size += sizeOfTransportVersion(version.nextPatchVersion());
        return size;
    }

    private static long sizeOfIndexVersion(IndexVersion indexVersion) {
        return RamUsageEstimator.shallowSizeOf(indexVersion) + RamUsageEstimator.shallowSizeOf(indexVersion.luceneVersion());
    }

    private static long sizeOfLifecycleExecutionState(@Nullable LifecycleExecutionState state) {
        if (state == null) {
            return 0L;
        }
        long size = RamUsageEstimator.shallowSizeOf(state);
        size += RamUsageEstimator.sizeOf(state.phase());
        size += RamUsageEstimator.sizeOf(state.action());
        size += RamUsageEstimator.sizeOf(state.step());
        size += RamUsageEstimator.sizeOf(state.failedStep());
        size += RamUsageEstimator.shallowSizeOf(state.isAutoRetryableError());
        size += RamUsageEstimator.shallowSizeOf(state.failedStepRetryCount());
        size += RamUsageEstimator.sizeOf(state.stepInfo());
        size += RamUsageEstimator.sizeOf(state.previousStepInfo());
        size += RamUsageEstimator.sizeOf(state.phaseDefinition());
        size += RamUsageEstimator.shallowSizeOf(state.lifecycleDate());
        size += RamUsageEstimator.shallowSizeOf(state.phaseTime());
        size += RamUsageEstimator.shallowSizeOf(state.actionTime());
        size += RamUsageEstimator.shallowSizeOf(state.stepTime());
        size += RamUsageEstimator.sizeOf(state.snapshotRepository());
        size += RamUsageEstimator.sizeOf(state.snapshotName());
        size += RamUsageEstimator.sizeOf(state.shrinkIndexName());
        size += RamUsageEstimator.sizeOf(state.snapshotIndexName());
        size += RamUsageEstimator.sizeOf(state.downsampleIndexName());
        size += RamUsageEstimator.sizeOf(state.forceMergeCloneIndexName());
        return size;
    }

    private static long estimateDiscoveryNodeFiltersHeap(@Nullable DiscoveryNodeFilters filters) {
        if (filters == null) {
            return 0L;
        }
        long size = RamUsageEstimator.shallowSizeOf(filters);
        size += RamUsageEstimator.sizeOfMap(filters.getFilters());
        DiscoveryNodeFilters withoutTierPreferences = DiscoveryNodeFilters.trimTier(filters);
        if (withoutTierPreferences != null && withoutTierPreferences != filters) {
            size += estimateDiscoveryNodeFiltersHeap(withoutTierPreferences);
        }
        return size;
    }

    private static long estimateIndexLongFieldRangeHeap(@Nullable IndexLongFieldRange range) {
        if (range == null) {
            return 0L;
        }
        long size = RamUsageEstimator.shallowSizeOf(range);
        if (range.isComplete() == false) {
            size += sizeOfPrimitiveArray(range.numberOfTrackedShards(), Integer.BYTES);
        }
        return size;
    }

    private static long estimateIndexWriteLoadHeap(IndexWriteLoad writeLoad) {
        long size = RamUsageEstimator.shallowSizeOf(writeLoad);
        int numShards = writeLoad.numberOfShards();
        size += sizeOfPrimitiveArray(numShards, Double.BYTES) * 3;
        size += sizeOfPrimitiveArray(numShards, Long.BYTES);
        return size;
    }

    private static long estimateIndexMetadataStatsHeap(@Nullable IndexMetadataStats stats) {
        if (stats == null) {
            return 0L;
        }
        long size = RamUsageEstimator.shallowSizeOf(stats);
        size += estimateIndexWriteLoadHeap(stats.writeLoad());
        size += RamUsageEstimator.shallowSizeOf(stats.averageShardSize());
        return size;
    }

    private static long sizeOfPrimitiveArray(int length, int bytesPerElement) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * bytesPerElement);
    }

    private static long estimateInferenceFieldsHeap(Map<String, InferenceFieldMetadata> inferenceFields) {
        long size = RamUsageEstimator.shallowSizeOf(inferenceFields);
        long sizeOfEntry = -1;
        for (Map.Entry<String, InferenceFieldMetadata> entry : inferenceFields.entrySet()) {
            if (sizeOfEntry == -1) {
                sizeOfEntry = RamUsageEstimator.shallowSizeOf(entry);
            }
            size += sizeOfEntry;
            size += RamUsageEstimator.sizeOf(entry.getKey());
            size += entry.getValue().ramBytesUsed();
        }
        return RamUsageEstimator.alignObjectSize(size);
    }

    private static long estimateIndexReshardingMetadataHeap(@Nullable IndexReshardingMetadata reshardingMetadata) {
        if (reshardingMetadata == null) {
            return 0L;
        }
        long size = RamUsageEstimator.shallowSizeOf(reshardingMetadata);
        if (reshardingMetadata.isSplit()) {
            IndexReshardingState.Split split = reshardingMetadata.getSplit();
            size += RamUsageEstimator.shallowSizeOf(split);
            size += RamUsageEstimator.shallowSizeOf(split.sourceShards());
            size += RamUsageEstimator.shallowSizeOf(split.targetShards());
        } else {
            size += RamUsageEstimator.shallowSizeOf(reshardingMetadata.getState());
        }
        return size;
    }
}
