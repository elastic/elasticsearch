/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.Set;
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
                    FieldStats stats = fieldTypes.computeIfAbsent(type, FieldStats::new);
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
        return new MappingStats(fieldTypes.values(), runtimeFieldTypes.values());
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

    private final List<FieldStats> fieldTypeStats;
    private final List<RuntimeFieldStats> runtimeFieldStats;

    MappingStats(Collection<FieldStats> fieldTypeStats, Collection<RuntimeFieldStats> runtimeFieldStats) {
        List<FieldStats> stats = new ArrayList<>(fieldTypeStats);
        stats.sort(Comparator.comparing(IndexFeatureStats::getName));
        this.fieldTypeStats = Collections.unmodifiableList(stats);
        List<RuntimeFieldStats> runtimeStats = new ArrayList<>(runtimeFieldStats);
        runtimeStats.sort(Comparator.comparing(RuntimeFieldStats::type));
        this.runtimeFieldStats = Collections.unmodifiableList(runtimeStats);
    }

    MappingStats(StreamInput in) throws IOException {
        fieldTypeStats = Collections.unmodifiableList(in.readList(FieldStats::new));
        runtimeFieldStats = Collections.unmodifiableList(in.readList(RuntimeFieldStats::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(fieldTypeStats);
        out.writeCollection(runtimeFieldStats);
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
        if (o instanceof MappingStats == false) {
            return false;
        }
        MappingStats that = (MappingStats) o;
        return fieldTypeStats.equals(that.fieldTypeStats) && runtimeFieldStats.equals(that.runtimeFieldStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldTypeStats, runtimeFieldStats);
    }
}
