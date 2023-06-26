/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Usage statistics about mappings usage.
 */
public final class MappingStats implements ToXContentFragment, Writeable {

    private static final Pattern DOC_PATTERN = Pattern.compile("doc[\\[.]");
    private static final Pattern SOURCE_PATTERN = Pattern.compile("params\\._source");

    /**
     * Create {@link MappingStats} from the given cluster state.
     */
    public static MappingStats of(Metadata metadata, Runnable ensureNotCancelled) {
        Map<String, FieldStats> fieldTypes = new HashMap<>();
        Set<String> concreteFieldNames = new HashSet<>();
        Map<String, RuntimeFieldStats> runtimeFieldTypes = new HashMap<>();
        final Map<MappingMetadata, Integer> mappingCounts = new IdentityHashMap<>(metadata.getMappingsByHash().size());
        for (IndexMetadata indexMetadata : metadata) {
            if (indexMetadata.isSystem()) {
                // Don't include system indices in statistics about mappings,
                // we care about the user's indices.
                continue;
            }
            AnalysisStats.countMapping(mappingCounts, indexMetadata);
        }
        final AtomicLong totalFieldCount = new AtomicLong();
        final AtomicLong totalDeduplicatedFieldCount = new AtomicLong();
        for (Map.Entry<MappingMetadata, Integer> mappingAndCount : mappingCounts.entrySet()) {
            ensureNotCancelled.run();
            Set<String> indexFieldTypes = new HashSet<>();
            Set<String> indexRuntimeFieldTypes = new HashSet<>();
            final int count = mappingAndCount.getValue();
            final Map<String, Object> map = mappingAndCount.getKey().getSourceAsMap();
            MappingVisitor.visitMapping(map, (field, fieldMapping) -> {
                concreteFieldNames.add(field);
                String type = null;
                Object typeO = fieldMapping.get("type");
                if (typeO != null) {
                    type = typeO.toString();
                } else if (fieldMapping.containsKey("properties")) {
                    type = "object";
                }
                if (type != null) {
                    totalDeduplicatedFieldCount.incrementAndGet();
                    totalFieldCount.addAndGet(count);
                    FieldStats stats;
                    if (type.equals("dense_vector")) {
                        stats = fieldTypes.computeIfAbsent(type, DenseVectorFieldStats::new);
                        boolean indexed = fieldMapping.containsKey("index") ? (boolean) fieldMapping.get("index") : false;
                        if (indexed) {
                            ((DenseVectorFieldStats) stats).indexedVectorCount += count;
                            int dims = (int) fieldMapping.get("dims");
                            if (dims < ((DenseVectorFieldStats) stats).indexedVectorDimMin) {
                                ((DenseVectorFieldStats) stats).indexedVectorDimMin = dims;
                            }
                            if (dims > ((DenseVectorFieldStats) stats).indexedVectorDimMax) {
                                ((DenseVectorFieldStats) stats).indexedVectorDimMax = dims;
                            }
                        }
                    } else {
                        stats = fieldTypes.computeIfAbsent(type, FieldStats::new);
                    }
                    stats.count += count;
                    if (indexFieldTypes.add(type)) {
                        stats.indexCount += count;
                    }
                    Object scriptObject = fieldMapping.get("script");
                    if (scriptObject instanceof Map<?, ?> script) {
                        Object sourceObject = script.get("source");
                        stats.scriptCount += count;
                        updateScriptParams(sourceObject, stats.fieldScriptStats, count);
                        Object langObject = script.get("lang");
                        if (langObject != null) {
                            stats.scriptLangs.add(langObject.toString());
                        }
                    }
                }
            });

            MappingVisitor.visitRuntimeMapping(map, (field, fieldMapping) -> {
                Object typeObject = fieldMapping.get("type");
                if (typeObject == null) {
                    return;
                }
                String type = typeObject.toString();
                RuntimeFieldStats stats = runtimeFieldTypes.computeIfAbsent(type, RuntimeFieldStats::new);
                stats.count += count;
                if (indexRuntimeFieldTypes.add(type)) {
                    stats.indexCount += count;
                }
                if (concreteFieldNames.contains(field)) {
                    stats.shadowedCount += count;
                }
                Object scriptObject = fieldMapping.get("script");
                if (scriptObject == null) {
                    stats.scriptLessCount += count;
                } else if (scriptObject instanceof Map<?, ?> script) {
                    Object sourceObject = script.get("source");
                    updateScriptParams(sourceObject, stats.fieldScriptStats, count);
                    Object langObject = script.get("lang");
                    if (langObject != null) {
                        stats.scriptLangs.add(langObject.toString());
                    }
                }
            });
        }
        long totalMappingSizeBytes = 0L;
        for (MappingMetadata mappingMetadata : metadata.getMappingsByHash().values()) {
            totalMappingSizeBytes += mappingMetadata.source().compressed().length;
        }
        return new MappingStats(
            totalFieldCount.get(),
            totalDeduplicatedFieldCount.get(),
            totalMappingSizeBytes,
            fieldTypes.values(),
            runtimeFieldTypes.values()
        );
    }

    private static void updateScriptParams(Object scriptSourceObject, FieldScriptStats scriptStats, int multiplier) {
        if (scriptSourceObject != null) {
            String scriptSource = scriptSourceObject.toString();
            int chars = scriptSource.length();
            long lines = scriptSource.lines().count();
            int docUsages = countOccurrences(scriptSource, DOC_PATTERN);
            int sourceUsages = countOccurrences(scriptSource, SOURCE_PATTERN);
            scriptStats.update(chars, lines, sourceUsages, docUsages, multiplier);
        }
    }

    private static int countOccurrences(String script, Pattern pattern) {
        int occurrences = 0;
        Matcher matcher = pattern.matcher(script);
        while (matcher.find()) {
            occurrences++;
        }
        return occurrences;
    }

    @Nullable // for BwC
    private final Long totalFieldCount;

    @Nullable // for BwC
    private final Long totalDeduplicatedFieldCount;

    @Nullable // for BwC
    private final Long totalMappingSizeBytes;

    private final List<FieldStats> fieldTypeStats;
    private final List<RuntimeFieldStats> runtimeFieldStats;

    MappingStats(
        long totalFieldCount,
        long totalDeduplicatedFieldCount,
        long totalMappingSizeBytes,
        Collection<FieldStats> fieldTypeStats,
        Collection<RuntimeFieldStats> runtimeFieldStats
    ) {
        this.totalFieldCount = totalFieldCount;
        this.totalDeduplicatedFieldCount = totalDeduplicatedFieldCount;
        this.totalMappingSizeBytes = totalMappingSizeBytes;
        List<FieldStats> stats = new ArrayList<>(fieldTypeStats);
        stats.sort(Comparator.comparing(IndexFeatureStats::getName));
        this.fieldTypeStats = Collections.unmodifiableList(stats);
        List<RuntimeFieldStats> runtimeStats = new ArrayList<>(runtimeFieldStats);
        runtimeStats.sort(Comparator.comparing(RuntimeFieldStats::type));
        this.runtimeFieldStats = Collections.unmodifiableList(runtimeStats);
    }

    MappingStats(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            totalFieldCount = in.readOptionalVLong();
            totalDeduplicatedFieldCount = in.readOptionalVLong();
            totalMappingSizeBytes = in.readOptionalVLong();
        } else {
            totalFieldCount = null;
            totalDeduplicatedFieldCount = null;
            totalMappingSizeBytes = null;
        }
        fieldTypeStats = in.readImmutableList(FieldStats::new);
        runtimeFieldStats = in.readImmutableList(RuntimeFieldStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            out.writeOptionalVLong(totalFieldCount);
            out.writeOptionalVLong(totalDeduplicatedFieldCount);
            out.writeOptionalVLong(totalMappingSizeBytes);
        } // else just omit these stats, they're not computed on older nodes anyway
        out.writeCollection(fieldTypeStats);
        out.writeCollection(runtimeFieldStats);
    }

    private static OptionalLong ofNullable(Long l) {
        return l == null ? OptionalLong.empty() : OptionalLong.of(l);
    }

    /**
     * @return the total number of fields (in non-system indices), or {@link OptionalLong#empty()} if omitted (due to BwC)
     */
    public OptionalLong getTotalFieldCount() {
        return ofNullable(totalFieldCount);
    }

    /**
     * @return the total number of fields (in non-system indices) accounting for deduplication, or {@link OptionalLong#empty()} if omitted
     * (due to BwC)
     */
    public OptionalLong getTotalDeduplicatedFieldCount() {
        return ofNullable(totalDeduplicatedFieldCount);
    }

    /**
     * @return the total size of all mappings (including those for system indices) accounting for deduplication and compression, or {@link
     * OptionalLong#empty()} if omitted (due to BwC).
     */
    public OptionalLong getTotalMappingSizeBytes() {
        return ofNullable(totalMappingSizeBytes);
    }

    /**
     * Return stats about field types.
     */
    public List<FieldStats> getFieldTypeStats() {
        return fieldTypeStats;
    }

    /**
     * Return stats about runtime field types.
     */
    public List<RuntimeFieldStats> getRuntimeFieldStats() {
        return runtimeFieldStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("mappings");
        if (totalFieldCount != null) {
            builder.field("total_field_count", totalFieldCount);
        }
        if (totalDeduplicatedFieldCount != null) {
            builder.field("total_deduplicated_field_count", totalDeduplicatedFieldCount);
        }
        if (totalMappingSizeBytes != null) {
            builder.humanReadableField(
                "total_deduplicated_mapping_size_in_bytes",
                "total_deduplicated_mapping_size",
                ByteSizeValue.ofBytes(totalMappingSizeBytes)
            );
        }
        builder.startArray("field_types");
        for (IndexFeatureStats st : fieldTypeStats) {
            st.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray("runtime_field_types");
        for (RuntimeFieldStats st : runtimeFieldStats) {
            st.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MappingStats that = (MappingStats) o;
        return Objects.equals(totalFieldCount, that.totalFieldCount)
            && Objects.equals(totalDeduplicatedFieldCount, that.totalDeduplicatedFieldCount)
            && Objects.equals(totalMappingSizeBytes, that.totalMappingSizeBytes)
            && fieldTypeStats.equals(that.fieldTypeStats)
            && runtimeFieldStats.equals(that.runtimeFieldStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalFieldCount, totalDeduplicatedFieldCount, totalMappingSizeBytes, fieldTypeStats, runtimeFieldStats);
    }
}
