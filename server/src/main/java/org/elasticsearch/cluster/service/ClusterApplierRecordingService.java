/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats.Recording;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class ClusterApplierRecordingService {

    private static final Logger logger = LogManager.getLogger(ClusterApplierRecordingService.class);

    private final Map<String, MeanMetric> recordedActions = new HashMap<>();

    synchronized Stats getStats() {
        return new Stats(
            recordedActions.entrySet()
                .stream()
                .sorted(Comparator.<Map.Entry<String, MeanMetric>>comparingLong(o -> o.getValue().sum()).reversed())
                .collect(Maps.toUnmodifiableOrderedMap(Map.Entry::getKey, v -> new Recording(v.getValue().count(), v.getValue().sum())))
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
        private long startMillis;
        private boolean recording;
        private SubscribableListener<Void> currentListener;
        private final List<Tuple<String, Long>> recordings = new LinkedList<>();
        private final ThreadPool threadPool;
        private final TimeValue debugLoggingTimeout;

        Recorder(ThreadPool threadPool, TimeValue debugLoggingTimeout) {
            this.threadPool = threadPool;
            this.debugLoggingTimeout = debugLoggingTimeout;
        }

        Releasable record(String action) {
            if (recording) {
                throw new IllegalStateException("already recording");
            }

            this.recording = true;
            this.currentAction = action;
            this.startMillis = threadPool.rawRelativeTimeInMillis();

            if (logger.isDebugEnabled()) {
                currentListener = new SubscribableListener<>();
                currentListener.addTimeout(debugLoggingTimeout, threadPool, threadPool.generic());
                currentListener.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {}

                    @Override
                    public void onFailure(Exception e) {
                        assert e instanceof ElasticsearchTimeoutException : e; // didn't complete in time
                        HotThreads.logLocalHotThreads(
                            logger,
                            Level.DEBUG,
                            "hot threads while applying cluster state [" + currentAction + ']',
                            ReferenceDocs.LOGGING
                        );
                    }
                });
            }

            return this::stop;
        }

        void stop() {
            recording = false;
            long elapsedMillis = threadPool.rawRelativeTimeInMillis() - this.startMillis;
            recordings.add(new Tuple<>(currentAction, elapsedMillis));

            if (currentListener != null) {
                currentListener.onResponse(null);
                currentListener = null;
            }
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

        public Map<String, Recording> getRecordings() {
            return recordings;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readOrderedMap(StreamInput::readString, Recording::new));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("cluster_applier_stats");
            builder.startArray("recordings");
            for (Map.Entry<String, Recording> entry : recordings.entrySet()) {
                builder.startObject();
                builder.field("name", entry.getKey());
                String name = "cumulative_execution";
                builder.field(name + "_count", entry.getValue().count);
                builder.humanReadableField(name + "_time_millis", name + "_time", TimeValue.timeValueMillis(entry.getValue().sum));
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(recordings, StreamOutput::writeWriteable);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return Objects.equals(recordings, stats.recordings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordings);
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

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Recording recording = (Recording) o;
                return count == recording.count && sum == recording.sum;
            }

            @Override
            public int hashCode() {
                return Objects.hash(count, sum);
            }

            @Override
            public String toString() {
                return "Recording{" + "count=" + count + ", sum=" + sum + '}';
            }
        }
    }
}
