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
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

    /**
     * Create {@link MappingStats} from the given cluster state.
     */
    public static MappingStats of(Metadata metadata, Runnable ensureNotCancelled) {
        Map<String, IndexFeatureStats> fieldTypes = new HashMap<>();
        Set<String> concreteFieldNames = new HashSet<>();
        Map<String, RuntimeFieldStats> runtimeFieldTypes = new HashMap<>();
        for (IndexMetadata indexMetadata : metadata) {
            ensureNotCancelled.run();
            if (indexMetadata.isSystem()) {
                // Don't include system indices in statistics about mappings,
                // we care about the user's indices.
                continue;
            }
            Set<String> indexFieldTypes = new HashSet<>();
            Set<String> indexRuntimeFieldTypes = new HashSet<>();
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                MappingVisitor.visitMapping(mappingMetadata.getSourceAsMap(), (field, fieldMapping) -> {
                    concreteFieldNames.add(field);
                    String type = null;
                    Object typeO = fieldMapping.get("type");
                    if (typeO != null) {
                        type = typeO.toString();
                    } else if (fieldMapping.containsKey("properties")) {
                        type = "object";
                    }

                    if (type != null) {
                        IndexFeatureStats stats = fieldTypes.computeIfAbsent(type, IndexFeatureStats::new);
                        stats.count++;
                        if (indexFieldTypes.add(type)) {
                            stats.indexCount++;
                        }
                    }
                });

                MappingVisitor.visitRuntimeMapping(mappingMetadata.getSourceAsMap(), (field, fieldMapping) -> {
                    Object typeObject = fieldMapping.get("type");
                    if (typeObject == null) {
                        return;
                    }
                    String type = typeObject.toString();
                    RuntimeFieldStats stats = runtimeFieldTypes.computeIfAbsent(type, RuntimeFieldStats::new);
                    stats.count++;
                    if (indexRuntimeFieldTypes.add(type)) {
                        stats.indexCount++;
                    }
                    if (concreteFieldNames.contains(field)) {
                        stats.shadowedCount++;
                    }
                    Object scriptObject = fieldMapping.get("script");
                    if (scriptObject == null) {
                        stats.scriptLessCount++;
                    } else if (scriptObject instanceof Map) {
                        Map<?, ?> script = (Map<?, ?>) scriptObject;
                        Object sourceObject = script.get("source");
                        if (sourceObject != null) {
                            String scriptSource = sourceObject.toString();
                            int chars = scriptSource.length();
                            long lines = scriptSource.lines().count();
                            int docUsages = countOccurrences(scriptSource, "doc[\\[\\.]");
                            int sourceUsages = countOccurrences(scriptSource, "params\\._source");
                            stats.update(chars, lines, sourceUsages, docUsages);
                        }
                        Object langObject = script.get("lang");
                        if (langObject != null) {
                            stats.scriptLangs.add(langObject.toString());
                        }
                    }
                });
            }
        }
        return new MappingStats(fieldTypes.values(), runtimeFieldTypes.values());
    }

    private static int countOccurrences(String script, String keyword) {
        int occurrences = 0;
        Pattern pattern = Pattern.compile(keyword);
        Matcher matcher = pattern.matcher(script);
        while (matcher.find()) {
            occurrences++;
        }
        return occurrences;
    }

    private final Set<IndexFeatureStats> fieldTypeStats;
    private final Set<RuntimeFieldStats> runtimeFieldTypeStats;

    MappingStats(Collection<IndexFeatureStats> fieldTypeStats, Collection<RuntimeFieldStats> runtimeFieldTypeStats) {
        List<IndexFeatureStats> stats = new ArrayList<>(fieldTypeStats);
        stats.sort(Comparator.comparing(IndexFeatureStats::getName));
        this.fieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<IndexFeatureStats>(stats));
        List<RuntimeFieldStats> runtimeStats = new ArrayList<>(runtimeFieldTypeStats);
        runtimeStats.sort(Comparator.comparing(RuntimeFieldStats::type));
        this.runtimeFieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<>(runtimeStats));
    }

    MappingStats(StreamInput in) throws IOException {
        fieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<>(in.readList(IndexFeatureStats::new)));
        runtimeFieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<>(in.readList(RuntimeFieldStats::new)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(fieldTypeStats);
        out.writeCollection(runtimeFieldTypeStats);
    }

    /**
     * Return stats about field types.
     */
    public Set<IndexFeatureStats> getFieldTypeStats() {
        return fieldTypeStats;
    }

    /**
     * Return stats about runtime field types.
     */
    public Set<RuntimeFieldStats> getRuntimeFieldTypeStats() {
        return runtimeFieldTypeStats;
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
        for (RuntimeFieldStats st : runtimeFieldTypeStats) {
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
        return fieldTypeStats.equals(that.fieldTypeStats) &&
            runtimeFieldTypeStats.equals(that.runtimeFieldTypeStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldTypeStats, runtimeFieldTypeStats);
    }
}
