/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Records of the times the driver has slept.
 * @param counts map from the reason the driver has slept to the number of times it slept for that reason
 * @param first the first few times the driver slept
 * @param last the last few times the driver slept
 */
public record DriverSleeps(Map<String, Long> counts, List<Sleep> first, List<Sleep> last) implements Writeable, ToXContentObject {
    /**
     * A record of a time the driver slept.
     *
     * @param reason     The reason the driver slept
     * @param threadName The name of the thread this driver was running on when it went to sleep
     * @param sleep      Millis since epoch when the driver slept
     * @param wake       Millis since epoch when the driver woke, or 0 if it is currently sleeping
     */
    public record Sleep(String reason, String threadName, long sleep, long wake) implements Writeable, ToXContentObject {
        Sleep(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.getTransportVersion().onOrAfter(TransportVersions.ESQL_THREAD_NAME_IN_DRIVER_PROFILE) ? in.readString() : "",
                in.readLong(),
                in.readLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_THREAD_NAME_IN_DRIVER_PROFILE)) {
                out.writeString(threadName);
            }
            out.writeLong(sleep);
            out.writeLong(wake);
        }

        Sleep wake(long now) {
            if (isStillSleeping() == false) {
                throw new IllegalStateException("Already awake.");
            }
            return new Sleep(reason, Thread.currentThread().getName(), sleep, now);
        }

        public boolean isStillSleeping() {
            return wake == 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.field("thread_name", threadName);
            builder.timestampFieldsFromUnixEpochMillis("sleep_millis", "sleep", sleep);
            if (wake > 0) {
                builder.timestampFieldsFromUnixEpochMillis("wake_millis", "wake", wake);
            }
            return builder.endObject();
        }
    }

    /**
     * How many sleeps of the first and last sleeps and wakes to keep.
     */
    static final int RECORDS = 10;

    public static DriverSleeps read(StreamInput in) throws IOException {
        if (in.getTransportVersion().before(TransportVersions.V_8_16_0)) {
            return empty();
        }
        return new DriverSleeps(
            in.readImmutableMap(StreamInput::readVLong),
            in.readCollectionAsList(Sleep::new),
            in.readCollectionAsList(Sleep::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.V_8_16_0)) {
            return;
        }
        out.writeMap(counts, StreamOutput::writeVLong);
        out.writeCollection(first);
        out.writeCollection(last);
    }

    public static DriverSleeps empty() {
        return new DriverSleeps(Map.of(), List.of(), List.of());
    }

    /**
     * Record a sleep.
     * @param reason the reason for the sleep
     * @param now the current time
     */
    public DriverSleeps sleep(String reason, long now) {
        if (last.isEmpty() == false) {
            Sleep lastLast = last.get(last.size() - 1);
            if (lastLast.isStillSleeping()) {
                throw new IllegalStateException("Still sleeping.");
            }
        }
        Map<String, Long> newCounts = new TreeMap<>(counts);
        newCounts.compute(reason, (k, v) -> v == null ? 1 : v + 1);
        List<Sleep> newFirst = first.size() < RECORDS ? append(first, reason, now) : first;
        List<Sleep> newLast = last.size() < RECORDS ? append(last, reason, now) : rollOnto(last, reason, now);
        return new DriverSleeps(newCounts, newFirst, newLast);
    }

    /**
     * Record a wake.
     * @param now the current time
     */
    public DriverSleeps wake(long now) {
        if (now == 0) {
            throw new IllegalStateException("Can't wake at epoch. That's used to signal sleeping.");
        }
        if (last.isEmpty()) {
            throw new IllegalStateException("Never slept.");
        }
        Sleep lastFirst = first.get(first.size() - 1);
        List<Sleep> newFirst = lastFirst.wake == 0 ? wake(first, now) : first;
        return new DriverSleeps(counts, newFirst, wake(last, now));
    }

    private List<Sleep> append(List<Sleep> old, String reason, long now) {
        List<Sleep> sleeps = new ArrayList<>(old.size() + 1);
        sleeps.addAll(old);
        sleeps.add(new Sleep(reason, Thread.currentThread().getName(), now, 0));
        return Collections.unmodifiableList(sleeps);
    }

    private List<Sleep> rollOnto(List<Sleep> old, String reason, long now) {
        List<Sleep> sleeps = new ArrayList<>(old.size());
        for (int i = 1; i < old.size(); i++) {
            sleeps.add(old.get(i));
        }
        sleeps.add(new Sleep(reason, Thread.currentThread().getName(), now, 0));
        return Collections.unmodifiableList(sleeps);
    }

    private List<Sleep> wake(List<Sleep> old, long now) {
        List<Sleep> sleeps = new ArrayList<>(old);
        sleeps.set(sleeps.size() - 1, old.get(old.size() - 1).wake(now));
        return Collections.unmodifiableList(sleeps);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("counts");
        for (Map.Entry<String, Long> count : counts.entrySet()) {
            builder.field(count.getKey(), count.getValue());
        }
        builder.endObject();
        toXContent(builder, params, "first", first);
        toXContent(builder, params, "last", last);
        return builder.endObject();
    }

    private static void toXContent(XContentBuilder builder, ToXContent.Params params, String name, List<Sleep> sleeps) throws IOException {
        builder.startArray(name);
        for (Sleep sleep : sleeps) {
            sleep.toXContent(builder, params);
        }
        builder.endArray();
    }
}
