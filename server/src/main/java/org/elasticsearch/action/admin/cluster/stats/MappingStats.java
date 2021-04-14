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

    private static final Pattern DOC_PATTERN = Pattern.compile("doc[\\[.]");
    private static final Pattern SOURCE_PATTERN = Pattern.compile("params\\._source");

    /**
     * Create {@link MappingStats} from the given cluster state.
     */
    public static MappingStats of(Metadata metadata, Runnable ensureNotCancelled) {
        Map<String, FieldStats> fieldTypes = new HashMap<>();
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
                        FieldStats stats = fieldTypes.computeIfAbsent(type, FieldStats::new);
                        stats.count++;
                        if (indexFieldTypes.add(type)) {
                            stats.indexCount++;
                        }
                        Object scriptObject = fieldMapping.get("script");
                        if (scriptObject instanceof Map) {
                            Map<?, ?> script = (Map<?, ?>) scriptObject;
                            Object sourceObject = script.get("source");
                            stats.scriptCount++;
                            updateScriptParams(sourceObject, stats.fieldScriptStats);
                            Object langObject = script.get("lang");
                            if (langObject != null) {
                                stats.scriptLangs.add(langObject.toString());
                            }
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
                        updateScriptParams(sourceObject, stats.fieldScriptStats);
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

    private static void updateScriptParams(Object scriptSourceObject, FieldScriptStats scriptStats) {
        if (scriptSourceObject != null) {
            String scriptSource = scriptSourceObject.toString();
            int chars = scriptSource.length();
            long lines = scriptSource.lines().count();
            int docUsages = countOccurrences(scriptSource, DOC_PATTERN);
            int sourceUsages = countOccurrences(scriptSource, SOURCE_PATTERN);
            scriptStats.update(chars, lines, sourceUsages, docUsages);
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

    private final Set<FieldStats> fieldTypeStats;
    private final Set<RuntimeFieldStats> runtimeFieldStats;

    MappingStats(Collection<FieldStats> fieldTypeStats, Collection<RuntimeFieldStats> runtimeFieldStats) {
        List<FieldStats> stats = new ArrayList<>(fieldTypeStats);
        stats.sort(Comparator.comparing(IndexFeatureStats::getName));
        this.fieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<>(stats));
        List<RuntimeFieldStats> runtimeStats = new ArrayList<>(runtimeFieldStats);
        runtimeStats.sort(Comparator.comparing(RuntimeFieldStats::type));
        this.runtimeFieldStats = Collections.unmodifiableSet(new LinkedHashSet<>(runtimeStats));
    }

    MappingStats(StreamInput in) throws IOException {
        fieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<>(in.readList(FieldStats::new)));
        runtimeFieldStats = Collections.unmodifiableSet(new LinkedHashSet<>(in.readList(RuntimeFieldStats::new)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(fieldTypeStats);
        out.writeCollection(runtimeFieldStats);
    }

    /**
     * Return stats about field types.
     */
    public Set<FieldStats> getFieldTypeStats() {
        return fieldTypeStats;
    }

    /**
     * Return stats about runtime field types.
     */
    public Set<RuntimeFieldStats> getRuntimeFieldStats() {
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
        return fieldTypeStats.equals(that.fieldTypeStats) &&
            runtimeFieldStats.equals(that.runtimeFieldStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldTypeStats, runtimeFieldStats);
    }
}
