/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;

public class AggregationInfo implements ReportingService.Info {

    private final Map<String, Set<String>> aggs;

    AggregationInfo(Map<String, Map<String, LongAdder>> aggs) {
        // we use a treemap/treeset here to have a test-able / predictable order
        Map<String, Set<String>> aggsMap = new TreeMap<>();
        aggs.forEach((s, m) -> aggsMap.put(s, Collections.unmodifiableSet(new TreeSet<>(m.keySet()))));
        this.aggs = Collections.unmodifiableMap(aggsMap);
    }

    /**
     * Read from a stream.
     */
    public AggregationInfo(StreamInput in) throws IOException {
        aggs = new TreeMap<>();
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            final int keys = in.readVInt();
            final Set<String> types = new TreeSet<>();
            for (int j = 0; j < keys; j++) {
                types.add(in.readString());
            }
            aggs.put(key, types);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(aggs.size());
        for (Map.Entry<String, Set<String>> e : aggs.entrySet()) {
            out.writeString(e.getKey());
            out.writeVInt(e.getValue().size());
            for (String type : e.getValue()) {
                out.writeString(type);
            }
        }
    }

    public Map<String, Set<String>> getAggregations() {
        return aggs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("aggregations");
        for (Map.Entry<String, Set<String>> e : aggs.entrySet()) {
            builder.startObject(e.getKey());
            builder.startArray("types");
            for (String s : e.getValue()) {
                builder.value(s);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregationInfo that = (AggregationInfo) o;
        return Objects.equals(aggs, that.aggs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggs);
    }
}
