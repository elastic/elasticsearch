/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ClusterApplierTimeTracker {

    // Most listeners are added when the node is initialized, however a few
    // are added during the lifetime of a node and for these cases we need ConcurrentHashMap
    private final Map<String, CounterMetric> timeSpentPerApplier = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> timeSpentPerListener = new ConcurrentHashMap<>();

    void addApplier(String name) {
        timeSpentPerApplier.put(name, new CounterMetric());
    }

    void removeApplier(String name) {
        timeSpentPerApplier.remove(name);
    }

    void addListener(String name) {
        timeSpentPerListener.put(name, new CounterMetric());
    }

    void removeListener(String name) {
        timeSpentPerListener.remove(name);
    }

    void incrementTimeSpentApplier(String name, TimeValue took) {
        increment(timeSpentPerApplier.get(name), took);
    }

    void incrementTimeSpentListener(String name, TimeValue took) {
        increment(timeSpentPerListener.get(name), took);
    }

    private static void increment(CounterMetric counter, TimeValue took) {
//        assert counter != null : "no counter for [" + name + "]";
        if (counter != null) {
            // Listeners/appliers may get removed while executing.
            counter.inc(took.millis());
        }
    }

    Stats getStats() {
        return new Stats(
            timeSpentPerApplier.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().count())),
            timeSpentPerListener.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().count()))
        );
    }

    public static class Stats implements Writeable, ToXContentFragment {

        private final Map<String, Long> timeSpentPerApplier;
        private final Map<String, Long> timeSpentPerListener;

        public Stats(Map<String, Long> timeSpentPerApplier, Map<String, Long> timeSpentPerListener) {
            this.timeSpentPerApplier = timeSpentPerApplier;
            this.timeSpentPerListener = timeSpentPerListener;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readMap(StreamInput::readString, StreamInput::readVLong), in.readMap(StreamInput::readString, StreamInput::readVLong));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("cluster_applier_stats");
            builder.startArray("appliers");
            toXContentTimeSpent(builder, timeSpentPerApplier);
            builder.startArray("listeners");
            toXContentTimeSpent(builder, timeSpentPerListener);
            builder.endObject();
            return builder;
        }

        private static void toXContentTimeSpent(XContentBuilder builder, Map<String, Long> timeSpentPerListener) throws IOException {
            timeSpentPerListener.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEach(entry -> {
                    try {
                        builder.startObject();
                        builder.field("name", entry.getKey());
                        String name = "cumulative_execution";
                        builder.humanReadableField(name + "_time_millis", name + "_time", TimeValue.timeValueMillis(entry.getValue()));
                        builder.endObject();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            builder.endArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(timeSpentPerApplier, StreamOutput::writeString, StreamOutput::writeVLong);
            out.writeMap(timeSpentPerListener, StreamOutput::writeString, StreamOutput::writeVLong);
        }
    }
}
