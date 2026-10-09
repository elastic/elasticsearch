/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.network;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads the total bytes received and transmitted by this node's network interfaces (excluding loopback) from {@code /proc/net/dev}. In a
 * container this covers the container's network namespace.
 */
public class NetworkProbe {

    private static final Logger logger = LogManager.getLogger(NetworkProbe.class);

    private static final String LOOPBACK_INTERFACE = "lo";

    // /proc/net/dev has 8 receive columns followed by 8 transmit columns, the first of each being the byte count
    private static final int RECEIVE_BYTES_FIELD = 0;
    private static final int TRANSMIT_BYTES_FIELD = 8;
    private static final int MIN_FIELDS = 16;

    private static final NetworkProbe INSTANCE = new NetworkProbe();

    private final AtomicBoolean loggedUnavailable = new AtomicBoolean();

    public static NetworkProbe getInstance() {
        return INSTANCE;
    }

    NetworkProbe() {}

    /**
     * Cumulative byte counters summed over all non-loopback interfaces.
     */
    public record NetworkStats(long receiveBytes, long transmitBytes) {}

    /**
     * @return the current counters, or {@code null} if they are not available (not Linux, or {@code /proc/net/dev} unreadable)
     */
    @Nullable
    public NetworkStats getNetworkStats() {
        if (Constants.LINUX == false) {
            logUnavailableOnce(null);
            return null;
        }
        try {
            final NetworkStats stats = parseProcNetDev(readProcNetDev());
            if (stats == null) {
                logUnavailableOnce(null);
            }
            return stats;
        } catch (Exception e) {
            logUnavailableOnce(e);
            return null;
        }
    }

    private void logUnavailableOnce(@Nullable Exception e) {
        if (loggedUnavailable.compareAndSet(false, true)) {
            logger.debug("network stats from /proc/net/dev are not available", e);
        }
    }

    @SuppressForbidden(reason = "read /proc/net/dev")
    List<String> readProcNetDev() throws IOException {
        return Files.readAllLines(PathUtils.get("/proc/net/dev"));
    }

    /**
     * Parses the contents of {@code /proc/net/dev}, skipping the header, the loopback interface and malformed lines.
     *
     * @return the summed counters, or {@code null} if no interface line could be parsed
     */
    @Nullable
    static NetworkStats parseProcNetDev(List<String> lines) {
        long receiveBytes = 0;
        long transmitBytes = 0;
        boolean parsedAny = false;
        for (String line : lines) {
            final int colon = line.indexOf(':');
            if (colon < 0) {
                continue; // header lines
            }
            final String name = line.substring(0, colon).trim();
            final String[] fields = line.substring(colon + 1).trim().split("\\s+");
            if (name.isEmpty() || fields.length < MIN_FIELDS) {
                continue;
            }
            final long rx;
            final long tx;
            try {
                rx = Long.parseLong(fields[RECEIVE_BYTES_FIELD]);
                tx = Long.parseLong(fields[TRANSMIT_BYTES_FIELD]);
            } catch (NumberFormatException e) {
                continue;
            }
            if (rx < 0 || tx < 0) {
                continue;
            }
            parsedAny = true;
            if (LOOPBACK_INTERFACE.equals(name) == false) {
                receiveBytes += rx;
                transmitBytes += tx;
            }
        }
        return parsedAny ? new NetworkStats(receiveBytes, transmitBytes) : null;
    }
}
