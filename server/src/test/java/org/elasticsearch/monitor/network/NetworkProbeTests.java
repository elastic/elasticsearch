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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NetworkProbeTests extends ESTestCase {

    private static final String HEADER_1 = "Inter-|   Receive                                                |  Transmit";
    private static final String HEADER_2 =
        " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed";

    public void testParseSumsInterfacesExceptLoopback() {
        final List<String> lines = List.of(
            HEADER_1,
            HEADER_2,
            "    lo: 9999999    1000    0    0    0     0          0         0  8888888    1000    0    0    0     0       0          0",
            "  eth0: 1000       10    0    0    0     0          0         0     2000      20    0    0    0     0       0          0",
            "  eth1:300         3    0    0    0     0          0         0      400       4    0    0    0     0       0          0"
        );
        final NetworkProbe.NetworkStats stats = NetworkProbe.parseProcNetDev(lines);
        assertThat(stats, notNullValue());
        assertThat(stats.receiveBytes(), equalTo(1300L));
        assertThat(stats.transmitBytes(), equalTo(2400L));
    }

    public void testParseOnlyLoopback() {
        final NetworkProbe.NetworkStats stats = NetworkProbe.parseProcNetDev(
            List.of(HEADER_1, HEADER_2, "    lo: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0")
        );
        assertThat(stats, equalTo(new NetworkProbe.NetworkStats(0L, 0L)));
    }

    public void testParseSkipsMalformedLines() {
        final List<String> lines = List.of(
            HEADER_1,
            HEADER_2,
            "  eth0: 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0",
            "  eth1: 1000 10 0 0",                                  // too few fields
            "  eth2: abc 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0",        // not a number
            "  eth3: 1000 10 0 0 0 0 0 0 -5 20 0 0 0 0 0 0",         // negative
            "      : 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0",       // no name
            "garbage",
            ""
        );
        assertThat(NetworkProbe.parseProcNetDev(lines), equalTo(new NetworkProbe.NetworkStats(1000L, 2000L)));
    }

    public void testParseReturnsNullWithoutInterfaces() {
        assertThat(NetworkProbe.parseProcNetDev(List.of()), nullValue());
        assertThat(NetworkProbe.parseProcNetDev(List.of(HEADER_1, HEADER_2)), nullValue());
        assertThat(NetworkProbe.parseProcNetDev(List.of(HEADER_1, HEADER_2, "  eth0: 1 2 3")), nullValue());
    }

    public void testUnreadableReturnsNull() {
        final NetworkProbe probe = new NetworkProbe() {
            @Override
            List<String> readProcNetDev() throws IOException {
                throw new NoSuchFileException("/proc/net/dev");
            }
        };
        assertThat(probe.getNetworkStats(), nullValue());
        // logs only once but keeps returning null
        assertThat(probe.getNetworkStats(), nullValue());
    }

    public void testRealProbe() {
        final NetworkProbe.NetworkStats stats = NetworkProbe.getInstance().getNetworkStats();
        if (Constants.LINUX) {
            assertThat(stats, notNullValue());
            assertThat(stats.receiveBytes(), greaterThanOrEqualTo(0L));
            assertThat(stats.transmitBytes(), greaterThanOrEqualTo(0L));
        } else {
            assertThat(stats, nullValue());
        }
    }
}
