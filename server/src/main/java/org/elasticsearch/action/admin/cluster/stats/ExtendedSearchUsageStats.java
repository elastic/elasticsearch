/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Provides extended statistics for {@link SearchUsage} beyond the basic counts provided in {@link SearchUsageStats}.
 */
public class ExtendedSearchUsageStats implements Writeable, ToXContent {

    /**
     * A map of categories to extended data. Categories correspond to a high-level search usage statistic,
     * e.g. `queries`, `rescorers`, `sections`, `retrievers`.
     *
     * Extended data is further segmented by name, e.g., collecting specific statistics for certain retrievers only.
     */
    private final Map<String, Map<String, ExtendedSearchUsageMetric<?>>> categorizedExtendedData;

    public static final ExtendedSearchUsageStats EMPTY = new ExtendedSearchUsageStats();

    public ExtendedSearchUsageStats() {
        this.categorizedExtendedData = new HashMap<>();
    }

    public ExtendedSearchUsageStats(Map<String, Map<String, ExtendedSearchUsageMetric<?>>> categorizedExtendedData) {
        this.categorizedExtendedData = categorizedExtendedData;
    }

    public ExtendedSearchUsageStats(StreamInput in) throws IOException {
        this.categorizedExtendedData = in.readMap(
            StreamInput::readString,
            i -> i.readMap(StreamInput::readString, p -> p.readNamedWriteable(ExtendedSearchUsageMetric.class))
        );
    }

    public Map<String, Map<String, ExtendedSearchUsageMetric<?>>> getCategorizedExtendedData() {
        return Collections.unmodifiableMap(categorizedExtendedData);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(
            categorizedExtendedData,
            StreamOutput::writeString,
            (o, v) -> o.writeMap(v, StreamOutput::writeString, (p, q) -> out.writeNamedWriteable(q))
        );
    }

    public void merge(ExtendedSearchUsageStats other) {
        other.categorizedExtendedData.forEach((key, otherMap) -> {
            categorizedExtendedData.merge(key, otherMap, (existingMap, newMap) -> {
                Map<String, ExtendedSearchUsageMetric<?>> mergedMap = new HashMap<>(existingMap);
                newMap.forEach(
                    (innerKey, innerValue) -> { mergedMap.merge(innerKey, innerValue, (existing, incoming) -> (existing).merge(incoming)); }
                );
                return mergedMap;
            });
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        for (String category : categorizedExtendedData.keySet()) {
            builder.startObject(category);
            Map<String, ExtendedSearchUsageMetric<?>> names = categorizedExtendedData.get(category);
            for (String name : names.keySet()) {
                builder.startObject(name);
                names.get(name).toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtendedSearchUsageStats that = (ExtendedSearchUsageStats) o;
        return Objects.equals(categorizedExtendedData, that.categorizedExtendedData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categorizedExtendedData);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
