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
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ClusterApplierTimeTracker {

    // Most listeners are added when the node is initialized, however a few
    // are added during the lifetime of a node and for these cases we need ConcurrentHashMap
    private final Map<String, MeanMetric> timeSpentPerApplier = new ConcurrentHashMap<>();
    private final Map<String, MeanMetric> timeSpentPerListener = new ConcurrentHashMap<>();

    void addApplier(String name) {
        timeSpentPerApplier.put(name, new MeanMetric());
    }

    void removeApplier(String name) {
        timeSpentPerApplier.remove(name);
    }

    void addListener(String name) {
        timeSpentPerListener.put(name, new MeanMetric());
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

    private static void increment(MeanMetric counter, TimeValue took) {
//        assert counter != null : "no counter for [" + name + "]";
        if (counter != null) {
            // Listeners/appliers may get removed while executing.
            counter.inc(took.millis());
        }
    }

    Stats getStats() {
        return new Stats(
            timeSpentPerApplier.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> new Stats.Stat(e.getValue().count(), e.getValue().sum()))),
            timeSpentPerListener.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> new Stats.Stat(e.getValue().count(), e.getValue().sum())))
        );
    }

    public static class Stats implements Writeable, ToXContentFragment {

        private final Map<String, Stat> timeSpentPerApplier;
        private final Map<String, Stat> timeSpentPerListener;

        public Stats(Map<String, Stat> timeSpentPerApplier, Map<String, Stat> timeSpentPerListener) {
            this.timeSpentPerApplier = timeSpentPerApplier;
            this.timeSpentPerListener = timeSpentPerListener;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readMap(StreamInput::readString, Stat::new), in.readMap(StreamInput::readString, Stat::new));
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

        private static void toXContentTimeSpent(XContentBuilder builder, Map<String, Stat> timeSpentPerListener) throws IOException {
            timeSpentPerListener.entrySet().stream()
                .sorted((o1, o2) -> -Long.compare(o1.getValue().sum, o2.getValue().sum))
                .forEach(entry -> {
                    try {
                        builder.startObject();
                        builder.field("name", entry.getKey());
                        String name = "cumulative_execution";
                        builder.field(name + "_count", entry.getValue().count);
                        builder.humanReadableField(name + "_time_millis", name + "_time", TimeValue.timeValueMillis(entry.getValue().sum));
                        builder.endObject();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            builder.endArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(timeSpentPerApplier, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
            out.writeMap(timeSpentPerListener, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        }

        public static class Stat implements Writeable {

            private final long count;
            private final long sum;

            public Stat(long count, long sum) {
                this.count = count;
                this.sum = sum;
            }

            public Stat(StreamInput in) throws IOException {
                this(in.readVLong(), in.readVLong());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVLong(count);
                out.writeVLong(sum);
            }
        }
    }
}
