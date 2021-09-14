/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats.Recording;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ClusterApplierRecordingService {

    private final Map<String, MeanMetric> recordedActions = new ConcurrentHashMap<>();

    synchronized Stats getStats() {
        return new Stats(
            recordedActions.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> new Recording(e.getValue().count(), e.getValue().sum())))
        );
    }

    synchronized void updateStats(Recorder recorder) {
        Set<String> seenActions = new HashSet<>();
        for (Tuple<String, Long> entry : recorder.recordings) {
            String action = entry.v1();
            long timeSpentMS = entry.v2();

            MeanMetric metric = recordedActions.computeIfAbsent(action, key -> new MeanMetric());
            metric.inc(timeSpentMS);
            seenActions.add(action);
        }
        recordedActions.entrySet().removeIf(entry -> seenActions.contains(entry.getKey()) == false);
    }

    static final class Recorder {

        private String currentAction;
        private long startTimeNS;
        private boolean recording;
        private final List<Tuple<String, Long>> recordings = new LinkedList<>();

        Releasable record(String action) {
            if (recording) {
                throw new IllegalStateException("already recording");
            }

            this.recording = true;
            this.currentAction = action;
            this.startTimeNS = System.nanoTime();
            return this::stop;
        }

        void stop() {
            recording = false;
            long timeSpentMS = TimeValue.nsecToMSec(System.nanoTime() - this.startTimeNS);
            recordings.add(new Tuple<>(currentAction, timeSpentMS));
        }

        List<Tuple<String, Long>> getRecordings() {
            return recordings;
        }
    }

    public static class Stats implements Writeable, ToXContentFragment {

        private final Map<String, Recording> recordings;

        public Stats(Map<String, Recording> recordings) {
            this.recordings = recordings;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readMap(StreamInput::readString, Recording::new));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("cluster_applier_stats");
            builder.startArray("recordings");
            toXContentTimeSpent(builder, recordings);
            builder.endObject();
            return builder;
        }

        private static void toXContentTimeSpent(XContentBuilder builder, Map<String, Recording> timeSpentPerListener) throws IOException {
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
            out.writeMap(recordings, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        }

        public static class Recording implements Writeable {

            private final long count;
            private final long sum;

            public Recording(long count, long sum) {
                this.count = count;
                this.sum = sum;
            }

            public Recording(StreamInput in) throws IOException {
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
