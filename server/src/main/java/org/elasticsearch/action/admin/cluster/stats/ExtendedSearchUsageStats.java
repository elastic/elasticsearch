/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Provides extended statistics for {@link SearchUsage} beyond the basic counts provided in {@link SearchUsageStats}.
 */
public class ExtendedSearchUsageStats implements Writeable, ToXContent {

    /**
     * A map of categories to extended data. Categories correspond to a high-level search usage statistic,
     * e.g. `queries`, `rescorers`, `sections`, `retrievers`.
     *
     * Extended data is further segmented by name, for example collecting specific statistics for certain retrievers only.
     *
     * Finally, we have string:count pairs that track each individual metric we wish to track.
     */
    private final Map<String, Map<String,Map<String,Long>>> categoriesToExtendedData;

    public ExtendedSearchUsageStats() {
        this.categoriesToExtendedData = new HashMap<>();
    }

    public ExtendedSearchUsageStats(Map<String, Map<String,Map<String,Long>>> categoriesToExtendedData) {
        this.categoriesToExtendedData = categoriesToExtendedData;
    }

    public ExtendedSearchUsageStats(StreamInput in) throws IOException {
        this.categoriesToExtendedData = in.readMap(StreamInput::readString, i -> i.readMap(StreamInput::readString,
            j -> j.readMap(StreamInput::readString, StreamInput::readLong)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(
            categoriesToExtendedData, StreamOutput::writeString, (o, v)
            -> o.writeMap(v, StreamOutput::writeString, (p, q) -> p.writeMap(q, StreamOutput::writeString, StreamOutput::writeLong)));
    }

    public void merge(ExtendedSearchUsageStats other) {
        other.categoriesToExtendedData.forEach((key, otherMap) -> {
            categoriesToExtendedData.merge(key, otherMap, (existingMap, newMap) -> {
                Map<String, Map<String,Long>> mergedMap = new HashMap<>(existingMap);
                newMap.forEach((innerKey, innerValue) -> {
                    mergedMap.merge(innerKey, innerValue, (existingInnerMap, newInnerMap) -> {
                        Map<String, Long> mergedInnerMap = new HashMap<>(existingInnerMap);
                        newInnerMap.forEach((propertyKey, propertyValue) -> {
                            mergedInnerMap.merge(propertyKey, propertyValue, Long::sum);
                        });
                        return mergedInnerMap;
                    });
                });
                return mergedMap;
            });
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
       for (String category : categoriesToExtendedData.keySet()) {
           builder.startObject(category);
           Map<String, Map<String,Long>> names = categoriesToExtendedData.get(category);
           for (String name : names.keySet()) {
               for (String property : names.get(name).keySet()) {
                   builder.field(property, names.get(name).get(property));
               }
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
        return Objects.equals(categoriesToExtendedData, that.categoriesToExtendedData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoriesToExtendedData);
    }
}
