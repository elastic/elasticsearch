/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.runtimefields;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RuntimeFieldsFeatureSetUsage extends XPackFeatureSet.Usage {

    public static RuntimeFieldsFeatureSetUsage fromMetadata(Iterable<IndexMetadata> metadata) {
        Map<String, RuntimeFieldStats> fieldTypes = new HashMap<>();
        for (IndexMetadata indexMetadata : metadata) {
            if (indexMetadata.isSystem()) {
                // Don't include system indices in statistics about mappings, we care about the user's indices.
                continue;
            }
            Set<String> indexFieldTypes = new HashSet<>();
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                Object runtimeObject = mappingMetadata.getSourceAsMap().get("runtime");
                if (runtimeObject instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> runtimeMappings = (Map<String, Object>) runtimeObject;
                    for (Object runtimeFieldMappingObject : runtimeMappings.values()) {
                        if (runtimeFieldMappingObject instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> runtimeFieldMapping = (Map<String, Object>) runtimeFieldMappingObject;
                            Object typeObject = runtimeFieldMapping.get("type");
                            if (typeObject != null) {
                                String type = typeObject.toString();
                                RuntimeFieldStats stats = fieldTypes.computeIfAbsent(type, RuntimeFieldStats::new);
                                stats.count++;
                                if (indexFieldTypes.add(type)) {
                                    stats.indexCount++;
                                }
                                Object scriptObject = runtimeFieldMapping.get("script");
                                if (scriptObject == null) {
                                    stats.scriptLessCount++;
                                } else if (scriptObject instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> script = (Map<String, Object>) scriptObject;
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
                            }
                        }
                    }
                }
            }
        }
        RuntimeFieldStats[] runtimeFieldStats = fieldTypes.values().toArray(new RuntimeFieldStats[0]);
        Arrays.sort(runtimeFieldStats, Comparator.comparing(RuntimeFieldStats::type));
        return new RuntimeFieldsFeatureSetUsage(runtimeFieldStats);
    }

    private final RuntimeFieldStats[] stats;

    RuntimeFieldsFeatureSetUsage(RuntimeFieldStats[] stats) {
        super(XPackField.RUNTIME_FIELDS, true, true);
        this.stats = stats;
    }

    public RuntimeFieldsFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.stats = in.readArray(RuntimeFieldStats::new, RuntimeFieldStats[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(stats);
    }

    RuntimeFieldStats[] getRuntimeFieldStats() {
        return stats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.startArray("field_types");
        for (RuntimeFieldStats stats : stats) {
            stats.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_11_0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RuntimeFieldsFeatureSetUsage that = (RuntimeFieldsFeatureSetUsage) o;
        return Arrays.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(stats);
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

    static final class RuntimeFieldStats implements Writeable, ToXContentObject {
        private final String type;
        private int count = 0;
        private int indexCount = 0;
        private final Set<String> scriptLangs;
        private long scriptLessCount = 0;
        private long minLines = Long.MAX_VALUE;
        private long maxLines = 0;
        private long totalLines = 0;
        private long minChars = Long.MAX_VALUE;
        private long maxChars = 0;
        private long totalChars = 0;
        private long minSourceUsages = Long.MAX_VALUE;
        private long maxSourceUsages = 0;
        private long totalSourceUsages = 0;
        private long minDocUsages = Long.MAX_VALUE;
        private long maxDocUsages = 0;
        private long totalDocUsages = 0;

        RuntimeFieldStats(String type) {
            this.type = Objects.requireNonNull(type);
            this.scriptLangs = new HashSet<>();
        }

        RuntimeFieldStats(StreamInput in) throws IOException {
            this.type = in.readString();
            this.count = in.readInt();
            this.indexCount = in.readInt();
            this.scriptLangs = in.readSet(StreamInput::readString);
            this.scriptLessCount = in.readLong();
            this.minLines = in.readLong();
            this.maxLines = in.readLong();
            this.totalLines = in.readLong();
            this.minChars = in.readLong();
            this.maxChars = in.readLong();
            this.totalChars = in.readLong();
            this.minSourceUsages = in.readLong();
            this.maxSourceUsages = in.readLong();
            this.totalSourceUsages = in.readLong();
            this.minDocUsages = in.readLong();
            this.maxDocUsages = in.readLong();
            this.totalDocUsages = in.readLong();
        }

        String type() {
            return type;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            out.writeInt(count);
            out.writeInt(indexCount);
            out.writeCollection(scriptLangs, StreamOutput::writeString);
            out.writeLong(scriptLessCount);
            out.writeLong(minLines);
            out.writeLong(maxLines);
            out.writeLong(totalLines);
            out.writeLong(minChars);
            out.writeLong(maxChars);
            out.writeLong(totalChars);
            out.writeLong(minSourceUsages);
            out.writeLong(maxSourceUsages);
            out.writeLong(totalSourceUsages);
            out.writeLong(minDocUsages);
            out.writeLong(maxDocUsages);
            out.writeLong(totalDocUsages);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", type);
            builder.field("count", count);
            builder.field("index_count", indexCount);
            builder.field("scriptless_count", scriptLessCount);
            builder.array("lang", scriptLangs.toArray(new String[0]));
            builder.field("lines_min", minLines);
            builder.field("lines_max", maxLines);
            builder.field("lines_total", totalLines);
            builder.field("chars_min", minChars);
            builder.field("chars_max", maxChars);
            builder.field("chars_total", totalChars);
            builder.field("source_min", minSourceUsages);
            builder.field("source_max", maxSourceUsages);
            builder.field("source_total", totalSourceUsages);
            builder.field("doc_min", minDocUsages);
            builder.field("doc_max", maxDocUsages);
            builder.field("doc_total", totalDocUsages);
            builder.endObject();
            return builder;
        }

        void update(int chars, long lines, int sourceUsages, int docUsages) {
            this.maxChars = Math.max(this.maxChars, chars);
            this.minChars = Math.min(this.minChars, chars);
            this.totalChars += chars;
            this.maxLines = Math.max(this.maxLines, lines);
            this.minLines = Math.min(this.minLines, lines);
            this.totalLines += lines;
            this.totalSourceUsages += sourceUsages;
            this.maxSourceUsages = Math.max(this.maxSourceUsages, sourceUsages);
            this.minSourceUsages = Math.min(this.minSourceUsages, sourceUsages);
            this.totalDocUsages += docUsages;
            this.maxDocUsages = Math.max(this.maxDocUsages, docUsages);
            this.minDocUsages = Math.min(this.minDocUsages, docUsages);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RuntimeFieldStats that = (RuntimeFieldStats) o;
            return count == that.count &&
                indexCount == that.indexCount &&
                scriptLessCount == that.scriptLessCount &&
                minLines == that.minLines &&
                maxLines == that.maxLines &&
                totalLines == that.totalLines &&
                minChars == that.minChars &&
                maxChars == that.maxChars &&
                totalChars == that.totalChars &&
                minSourceUsages == that.minSourceUsages &&
                maxSourceUsages == that.maxSourceUsages &&
                totalSourceUsages == that.totalSourceUsages &&
                minDocUsages == that.minDocUsages &&
                maxDocUsages == that.maxDocUsages &&
                totalDocUsages == that.totalDocUsages &&
                type.equals(that.type) &&
                scriptLangs.equals(that.scriptLangs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, count, indexCount, scriptLangs, scriptLessCount, minLines, maxLines, totalLines, minChars, maxChars,
                totalChars, minSourceUsages, maxSourceUsages, totalSourceUsages, minDocUsages, maxDocUsages, totalDocUsages);
        }
    }
}
