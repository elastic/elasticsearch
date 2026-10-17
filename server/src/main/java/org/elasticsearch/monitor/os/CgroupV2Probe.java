/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.os;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Reads what this node's own control group (in a container: the pod) consumed, from the cgroups v2 files {@code cpu.pressure} and
 * {@code cpu.stat}. The control group is located the same way as in {@link OsProbe}. Every reader returns
 * {@code null} if its file is not available (cgroups v1, no pressure stall information, not Linux), and says so once in the debug log.
 */
public class CgroupV2Probe {

    private static final Logger logger = LogManager.getLogger(CgroupV2Probe.class);

    private static final CgroupV2Probe INSTANCE = new CgroupV2Probe(OsProbe.getInstance());

    private final OsProbe osProbe;
    private final ConcurrentHashMap<String, Boolean> loggedUnavailable = new ConcurrentHashMap<>();

    public static CgroupV2Probe getInstance() {
        return INSTANCE;
    }

    CgroupV2Probe(OsProbe osProbe) {
        this.osProbe = osProbe;
    }

    /**
     * The cumulative time in microseconds in which at least one runnable task of the control group was waiting for CPU.
     */
    public record CpuPressure(long someTotalMicros) {}

    /**
     * Cumulative CFS throttling of the control group: the time in microseconds its tasks were throttled for exceeding the CPU quota,
     * and the number of periods in which that happened. Zero if the control group has no CPU controller, as it cannot be throttled.
     */
    public record CpuThrottling(long throttledMicros, long throttledPeriods) {}

    @Nullable
    public CpuPressure getCpuPressure() {
        return read("cpu.pressure", osProbe::readCgroupV2CpuPressure, CgroupV2Probe::parseCpuPressure);
    }

    @Nullable
    public CpuThrottling getCpuThrottling() {
        return read("cpu.stat", osProbe::readCgroupV2CpuStats, CgroupV2Probe::parseCpuThrottling);
    }

    @Nullable
    private <T> T read(String file, Reader reader, Function<List<String>, T> parser) {
        try {
            final String controlGroup = osProbe.getCgroupV2ControlGroup();
            if (controlGroup == null) {
                logUnavailableOnce(file, "not in a cgroups v2 hierarchy", null);
                return null;
            }
            final T result = parser.apply(reader.read(controlGroup));
            if (result == null) {
                logUnavailableOnce(file, "cannot be parsed", null);
            }
            return result;
        } catch (IOException | RuntimeException e) {
            logUnavailableOnce(file, "cannot be read", e);
            return null;
        }
    }

    private void logUnavailableOnce(String file, String reason, @Nullable Exception e) {
        if (loggedUnavailable.putIfAbsent(file, Boolean.TRUE) == null) {
            logger.debug("cgroup [{}] is not available: {}", file, reason);
            if (e != null) {
                logger.trace("reading cgroup [" + file + "] failed", e);
            }
        }
    }

    @FunctionalInterface
    private interface Reader {
        List<String> read(String controlGroup) throws IOException;
    }

    /**
     * Parses {@code cpu.pressure}, whose {@code some} line looks like {@code some avg10=0.00 avg60=0.00 avg300=0.00 total=12345}.
     *
     * @return the {@code total} of the {@code some} line, or {@code null} if there is no such line
     */
    @Nullable
    static CpuPressure parseCpuPressure(List<String> lines) {
        for (String line : lines) {
            final String[] fields = line.trim().split("\\s+");
            if (fields[0].equals("some")) {
                final long total = parseField(fields, "total");
                return total < 0 ? null : new CpuPressure(total);
            }
        }
        return null;
    }

    /**
     * Parses the throttling fields of {@code cpu.stat}, whose lines look like {@code throttled_usec 1234}. These are missing when the
     * control group has no CPU controller, which is read as no throttling.
     *
     * @return the throttling, or {@code null} if the file does not look like {@code cpu.stat} or has a malformed value
     */
    @Nullable
    static CpuThrottling parseCpuThrottling(List<String> lines) {
        boolean sawUsage = false;
        long throttledMicros = 0;
        long throttledPeriods = 0;
        for (String line : lines) {
            final String[] fields = line.trim().split("\\s+");
            if (fields.length != 2) {
                continue;
            }
            switch (fields[0]) {
                case "usage_usec" -> sawUsage = true;
                case "throttled_usec" -> throttledMicros = parseValue(fields[1]);
                case "nr_throttled" -> throttledPeriods = parseValue(fields[1]);
                default -> {
                }
            }
            if (throttledMicros < 0 || throttledPeriods < 0) {
                return null;
            }
        }
        return sawUsage ? new CpuThrottling(throttledMicros, throttledPeriods) : null;
    }

    /**
     * @return the value of the {@code key=value} field with the given key, or -1 if there is none or it is not a non-negative number
     */
    private static long parseField(String[] fields, String key) {
        final String prefix = key + "=";
        for (String field : fields) {
            if (field.startsWith(prefix)) {
                return parseValue(field.substring(prefix.length()));
            }
        }
        return -1L;
    }

    private static long parseValue(String value) {
        try {
            final long parsed = Long.parseLong(value);
            return parsed >= 0 ? parsed : -1L;
        } catch (NumberFormatException e) {
            return -1L;
        }
    }
}
