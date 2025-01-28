/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.os;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class OsProbeTests extends ESTestCase {

    public void testOsInfo() throws IOException {
        final int allocatedProcessors = randomIntBetween(1, Runtime.getRuntime().availableProcessors());
        final long refreshInterval = randomBoolean() ? -1 : randomNonNegativeLong();
        final String prettyName;
        if (Constants.LINUX) {
            prettyName = randomFrom("Fedora 28 (Workstation Edition)", "Linux", null);
        } else {
            prettyName = Constants.OS_NAME;
        }
        final OsProbe osProbe = new OsProbe() {

            @Override
            List<String> readOsRelease() {
                assert Constants.LINUX : Constants.OS_NAME;
                if (prettyName != null) {
                    final String quote = randomFrom("\"", "'", "");
                    final String space = randomFrom(" ", "");
                    final String prettyNameLine = Strings.format("PRETTY_NAME=%s%s%s%s", quote, prettyName, quote, space);
                    return Arrays.asList("NAME=" + randomAlphaOfLength(16), prettyNameLine);
                } else {
                    return Collections.singletonList("NAME=" + randomAlphaOfLength(16));
                }
            }

        };
        final OsInfo info = osProbe.osInfo(refreshInterval, Processors.of((double) allocatedProcessors));
        assertNotNull(info);
        assertThat(info.getRefreshInterval(), equalTo(refreshInterval));
        assertThat(info.getName(), equalTo(Constants.OS_NAME));
        if (Constants.LINUX) {
            if (prettyName != null) {
                assertThat(info.getPrettyName(), equalTo(prettyName));
            } else {
                assertThat(info.getPrettyName(), equalTo(Constants.OS_NAME));
            }
        }
        assertThat(info.getArch(), equalTo(Constants.OS_ARCH));
        assertThat(info.getVersion(), equalTo(Constants.OS_VERSION));
        assertThat(info.getAllocatedProcessors(), equalTo(allocatedProcessors));
        assertThat(info.getAvailableProcessors(), equalTo(Runtime.getRuntime().availableProcessors()));
    }

    public void testOsStats() {
        final OsProbe osProbe = new OsProbe();
        OsStats stats = osProbe.osStats();
        assertNotNull(stats);
        assertThat(stats.getTimestamp(), greaterThan(0L));
        assertThat(
            stats.getCpu().getPercent(),
            anyOf(equalTo((short) -1), is(both(greaterThanOrEqualTo((short) 0)).and(lessThanOrEqualTo((short) 100))))
        );
        double[] loadAverage = stats.getCpu().getLoadAverage();
        if (loadAverage != null) {
            assertThat(loadAverage.length, equalTo(3));
        }
        if (Constants.WINDOWS) {
            // load average is unavailable on Windows
            assertNull(loadAverage);
        } else if (Constants.LINUX) {
            // we should be able to get the load average
            assertNotNull(loadAverage);
            assertThat(loadAverage[0], greaterThanOrEqualTo((double) 0));
            assertThat(loadAverage[1], greaterThanOrEqualTo((double) 0));
            assertThat(loadAverage[2], greaterThanOrEqualTo((double) 0));
        } else if (Constants.MAC_OS_X) {
            // one minute load average is available, but 10-minute and 15-minute load averages are not
            assertNotNull(loadAverage);
            assertThat(loadAverage[0], greaterThanOrEqualTo((double) 0));
            assertThat(loadAverage[1], equalTo((double) -1));
            assertThat(loadAverage[2], equalTo((double) -1));
        } else {
            // unknown system, but the best case is that we have the one-minute load average
            if (loadAverage != null) {
                assertThat(loadAverage[0], anyOf(equalTo((double) -1), greaterThanOrEqualTo((double) 0)));
                assertThat(loadAverage[1], equalTo((double) -1));
                assertThat(loadAverage[2], equalTo((double) -1));
            }
        }

        assertNotNull(stats.getMem());
        assertThat(stats.getMem().getTotal().getBytes(), greaterThan(0L));
        assertThat(stats.getMem().getFree().getBytes(), greaterThan(0L));
        assertThat(stats.getMem().getFreePercent(), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100)));
        assertThat(stats.getMem().getUsed().getBytes(), greaterThan(0L));
        assertThat(stats.getMem().getUsedPercent(), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100)));

        assertNotNull(stats.getSwap());
        assertNotNull(stats.getSwap().getTotal());

        long total = stats.getSwap().getTotal().getBytes();
        if (total > 0) {
            assertThat(stats.getSwap().getTotal().getBytes(), greaterThan(0L));
            assertThat(stats.getSwap().getFree().getBytes(), greaterThanOrEqualTo(0L));
            assertThat(stats.getSwap().getUsed().getBytes(), greaterThanOrEqualTo(0L));
        } else {
            // On platforms with no swap
            assertThat(stats.getSwap().getTotal().getBytes(), equalTo(0L));
            assertThat(stats.getSwap().getFree().getBytes(), equalTo(0L));
            assertThat(stats.getSwap().getUsed().getBytes(), equalTo(0L));
        }

        if (Constants.LINUX) {
            if (stats.getCgroup() != null) {
                assertThat(stats.getCgroup().getCpuAcctControlGroup(), notNullValue());
                assertThat(stats.getCgroup().getCpuAcctUsageNanos(), greaterThan(BigInteger.ZERO));
                assertThat(stats.getCgroup().getCpuCfsQuotaMicros(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
                assertThat(stats.getCgroup().getCpuCfsPeriodMicros(), greaterThanOrEqualTo(0L));
                assertThat(stats.getCgroup().getCpuStat().getNumberOfElapsedPeriods(), greaterThanOrEqualTo(BigInteger.ZERO));
                assertThat(stats.getCgroup().getCpuStat().getNumberOfTimesThrottled(), greaterThanOrEqualTo(BigInteger.ZERO));
                assertThat(stats.getCgroup().getCpuStat().getTimeThrottledNanos(), greaterThanOrEqualTo(BigInteger.ZERO));
                // These could be null if transported from a node running an older version, but shouldn't be null on the current node
                assertThat(stats.getCgroup().getMemoryControlGroup(), notNullValue());
                String memoryLimitInBytes = stats.getCgroup().getMemoryLimitInBytes();
                assertThat(memoryLimitInBytes, notNullValue());
                if (memoryLimitInBytes.equals("max") == false) {
                    assertThat(new BigInteger(memoryLimitInBytes), greaterThan(BigInteger.ZERO));
                }
                assertThat(stats.getCgroup().getMemoryUsageInBytes(), notNullValue());
                assertThat(new BigInteger(stats.getCgroup().getMemoryUsageInBytes()), greaterThan(BigInteger.ZERO));
            }
        } else {
            assertNull(stats.getCgroup());
        }
    }

    public void testGetSystemLoadAverage() {
        assumeTrue("test runs on Linux only", Constants.LINUX);

        final OsProbe probe = new OsProbe() {
            @Override
            String readProcLoadavg() {
                return "1.51 1.69 1.99 3/417 23251";
            }
        };

        final double[] systemLoadAverage = probe.getSystemLoadAverage();

        assertNotNull(systemLoadAverage);
        assertThat(systemLoadAverage.length, equalTo(3));

        // avoid silliness with representing doubles
        assertThat(systemLoadAverage[0], equalTo(Double.parseDouble("1.51")));
        assertThat(systemLoadAverage[1], equalTo(Double.parseDouble("1.69")));
        assertThat(systemLoadAverage[2], equalTo(Double.parseDouble("1.99")));
    }

    public void testCgroupProbe() {
        final int availableCgroupsVersion = randomFrom(0, 1, 2);
        final String hierarchy = randomAlphaOfLength(16);

        final OsProbe probe = buildStubOsProbe(availableCgroupsVersion, hierarchy);

        final OsStats.Cgroup cgroup = probe.osStats().getCgroup();

        switch (availableCgroupsVersion) {
            case 0 -> assertNull(cgroup);
            case 1 -> {
                assertNotNull(cgroup);
                assertThat(cgroup.getCpuAcctControlGroup(), equalTo("/" + hierarchy));
                assertThat(cgroup.getCpuAcctUsageNanos(), equalTo(new BigInteger("364869866063112")));
                assertThat(cgroup.getCpuControlGroup(), equalTo("/" + hierarchy));
                assertThat(cgroup.getCpuCfsPeriodMicros(), equalTo(100000L));
                assertThat(cgroup.getCpuCfsQuotaMicros(), equalTo(50000L));
                assertThat(cgroup.getCpuStat().getNumberOfElapsedPeriods(), equalTo(BigInteger.valueOf(17992)));
                assertThat(cgroup.getCpuStat().getNumberOfTimesThrottled(), equalTo(BigInteger.valueOf(1311)));
                assertThat(cgroup.getCpuStat().getTimeThrottledNanos(), equalTo(new BigInteger("139298645489")));
                assertThat(cgroup.getMemoryLimitInBytes(), equalTo("18446744073709551615"));
                assertThat(cgroup.getMemoryUsageInBytes(), equalTo("4796416"));
            }
            case 2 -> {
                assertNotNull(cgroup);
                assertThat(cgroup.getCpuAcctControlGroup(), equalTo("/" + hierarchy));
                assertThat(cgroup.getCpuAcctUsageNanos(), equalTo(new BigInteger("364869866063000")));
                assertThat(cgroup.getCpuControlGroup(), equalTo("/" + hierarchy));
                assertThat(cgroup.getCpuCfsPeriodMicros(), equalTo(100000L));
                assertThat(cgroup.getCpuCfsQuotaMicros(), equalTo(50000L));
                assertThat(cgroup.getCpuStat().getNumberOfElapsedPeriods(), equalTo(BigInteger.valueOf(17992)));
                assertThat(cgroup.getCpuStat().getNumberOfTimesThrottled(), equalTo(BigInteger.valueOf(1311)));
                assertThat(cgroup.getCpuStat().getTimeThrottledNanos(), equalTo(new BigInteger("139298645000")));
                assertThat(cgroup.getMemoryLimitInBytes(), equalTo("18446744073709551615"));
                assertThat(cgroup.getMemoryUsageInBytes(), equalTo("4796416"));
            }
        }
    }

    public void testCgroupProbeWithMissingCpuAcct() {
        final String hierarchy = randomAlphaOfLength(16);

        // This cgroup data is missing a line about cpuacct
        List<String> procSelfCgroupLines = getProcSelfGroupLines(1, hierarchy).stream()
            .map(line -> line.replaceFirst(",cpuacct", ""))
            .toList();

        final OsProbe probe = buildStubOsProbe(1, hierarchy, procSelfCgroupLines);

        final OsStats.Cgroup cgroup = probe.osStats().getCgroup();

        assertNull(cgroup);
    }

    public void testCgroupProbeWithMissingCpu() {
        final String hierarchy = randomAlphaOfLength(16);

        // This cgroup data is missing a line about cpu
        List<String> procSelfCgroupLines = getProcSelfGroupLines(1, hierarchy).stream()
            .map(line -> line.replaceFirst(":cpu,", ":"))
            .toList();

        final OsProbe probe = buildStubOsProbe(1, hierarchy, procSelfCgroupLines);

        final OsStats.Cgroup cgroup = probe.osStats().getCgroup();

        assertNull(cgroup);
    }

    public void testCgroupProbeWithMissingMemory() {
        final String hierarchy = randomAlphaOfLength(16);

        // This cgroup data is missing a line about memory
        List<String> procSelfCgroupLines = getProcSelfGroupLines(1, hierarchy).stream()
            .filter(line -> line.contains(":memory:") == false)
            .toList();

        final OsProbe probe = buildStubOsProbe(1, hierarchy, procSelfCgroupLines);

        final OsStats.Cgroup cgroup = probe.osStats().getCgroup();

        assertNull(cgroup);
    }

    public void testGetTotalMemFromProcMeminfo() throws Exception {
        int cgroupsVersion = randomFrom(1, 2);

        // missing MemTotal line
        var meminfoLines = Arrays.asList(
            "MemFree:         8467692 kB",
            "MemAvailable:   39646240 kB",
            "Buffers:         4699504 kB",
            "Cached:         23290380 kB",
            "SwapCached:            0 kB",
            "Active:         43637908 kB",
            "Inactive:        8130280 kB"
        );
        OsProbe probe = buildStubOsProbe(cgroupsVersion, "", List.of(), meminfoLines);
        assertThat(probe.getTotalMemFromProcMeminfo(), equalTo(0L));

        // MemTotal line with invalid value
        meminfoLines = Arrays.asList(
            "MemTotal:        invalid kB",
            "MemFree:         8467692 kB",
            "MemAvailable:   39646240 kB",
            "Buffers:         4699504 kB",
            "Cached:         23290380 kB",
            "SwapCached:            0 kB",
            "Active:         43637908 kB",
            "Inactive:        8130280 kB"
        );
        probe = buildStubOsProbe(cgroupsVersion, "", List.of(), meminfoLines);
        assertThat(probe.getTotalMemFromProcMeminfo(), equalTo(0L));

        // MemTotal line with invalid unit
        meminfoLines = Arrays.asList(
            "MemTotal:       39646240 MB",
            "MemFree:         8467692 kB",
            "MemAvailable:   39646240 kB",
            "Buffers:         4699504 kB",
            "Cached:         23290380 kB",
            "SwapCached:            0 kB",
            "Active:         43637908 kB",
            "Inactive:        8130280 kB"
        );
        probe = buildStubOsProbe(cgroupsVersion, "", List.of(), meminfoLines);
        assertThat(probe.getTotalMemFromProcMeminfo(), equalTo(0L));

        // MemTotal line with random valid value
        long memTotalInKb = randomLongBetween(1, Long.MAX_VALUE / 1024L);
        meminfoLines = Arrays.asList(
            "MemTotal:        " + memTotalInKb + " kB",
            "MemFree:         8467692 kB",
            "MemAvailable:   39646240 kB",
            "Buffers:         4699504 kB",
            "Cached:         23290380 kB",
            "SwapCached:            0 kB",
            "Active:         43637908 kB",
            "Inactive:        8130280 kB"
        );
        probe = buildStubOsProbe(cgroupsVersion, "", List.of(), meminfoLines);
        assertThat(probe.getTotalMemFromProcMeminfo(), equalTo(memTotalInKb * 1024L));
    }

    public void testTotalMemoryOverride() {
        assertThat(OsProbe.getTotalMemoryOverride("123456789"), is(123456789L));
        assertThat(OsProbe.getTotalMemoryOverride("123456789123456789"), is(123456789123456789L));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> OsProbe.getTotalMemoryOverride("-1"));
        assertThat(e.getMessage(), is("Negative memory size specified in [es.total_memory_bytes]: [-1]"));
        e = expectThrows(IllegalArgumentException.class, () -> OsProbe.getTotalMemoryOverride("abc"));
        assertThat(e.getMessage(), is("Invalid value for [es.total_memory_bytes]: [abc]"));
        // Although numeric, this value overflows long. This won't be a problem in practice for sensible
        // overrides, as it will be a very long time before machines have more than 8 exabytes of RAM.
        e = expectThrows(IllegalArgumentException.class, () -> OsProbe.getTotalMemoryOverride("123456789123456789123456789"));
        assertThat(e.getMessage(), is("Invalid value for [es.total_memory_bytes]: [123456789123456789123456789]"));
    }

    public void testGetTotalMemoryOnDebian8() throws Exception {
        // tests the workaround for JDK bug on debian8: https://github.com/elastic/elasticsearch/issues/67089#issuecomment-756114654
        final OsProbe osProbe = new OsProbe();
        assumeTrue("runs only on Debian 8", osProbe.isDebian8());
        assertThat(osProbe.getTotalPhysicalMemorySize(), greaterThan(0L));
    }

    private static List<String> getProcSelfGroupLines(int cgroupsVersion, String hierarchy) {
        // It doesn't really matter if cgroupsVersion == 0 here

        if (cgroupsVersion == 2) {
            return List.of("0::/" + hierarchy);
        }

        return Arrays.asList(
            "10:freezer:/",
            "9:net_cls,net_prio:/",
            "8:pids:/",
            "7:blkio:/",
            "6:memory:/" + hierarchy,
            "5:devices:/user.slice",
            "4:hugetlb:/",
            "3:perf_event:/",
            "2:cpu,cpuacct,cpuset:/" + hierarchy,
            "1:name=systemd:/user.slice/user-1000.slice/session-2359.scope",
            "0::/cgroup2"
        );
    }

    private static OsProbe buildStubOsProbe(final int availableCgroupsVersion, final String hierarchy) {
        List<String> procSelfCgroupLines = getProcSelfGroupLines(availableCgroupsVersion, hierarchy);

        return buildStubOsProbe(availableCgroupsVersion, hierarchy, procSelfCgroupLines);
    }

    /**
     * Builds a test instance of OsProbe. Methods that ordinarily read from the filesystem are overridden to return values based upon
     * the arguments to this method.
     *
     * @param availableCgroupsVersion what version of cgroups are available, 1 or 2, or 0 for no cgroups. Normally OsProbe establishes this
     *                                for itself.
     * @param hierarchy a mock value used to generate a cgroup hierarchy.
     * @param procSelfCgroupLines the lines that will be used as the content of <code>/proc/self/cgroup</code>
     * @param procMeminfoLines lines that will be used as the content of <code>/proc/meminfo</code>
     * @return a test instance
     */
    private static OsProbe buildStubOsProbe(
        final int availableCgroupsVersion,
        final String hierarchy,
        List<String> procSelfCgroupLines,
        List<String> procMeminfoLines
    ) {
        return new OsProbe() {
            @Override
            OsStats.Cgroup getCgroup(boolean isLinux) {
                // Pretend we're always on Linux so that we can run the cgroup tests
                return super.getCgroup(true);
            }

            @Override
            List<String> readProcSelfCgroup() {
                return procSelfCgroupLines;
            }

            @Override
            String readSysFsCgroupCpuAcctCpuAcctUsage(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return "364869866063112";
            }

            @Override
            String readSysFsCgroupCpuAcctCpuCfsPeriod(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return "100000";
            }

            @Override
            String readSysFsCgroupCpuAcctCpuAcctCfsQuota(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return "50000";
            }

            @Override
            List<String> readSysFsCgroupCpuAcctCpuStat(String controlGroup) {
                return Arrays.asList("nr_periods 17992", "nr_throttled 1311", "throttled_time 139298645489");
            }

            @Override
            String readSysFsCgroupMemoryLimitInBytes(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                // This is the highest value that can be stored in an unsigned 64 bit number, hence too big for long
                return "18446744073709551615";
            }

            @Override
            String readSysFsCgroupMemoryUsageInBytes(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return "4796416";
            }

            @Override
            boolean areCgroupStatsAvailable() {
                return availableCgroupsVersion > 0;
            }

            @Override
            List<String> readProcMeminfo() {
                return procMeminfoLines;
            }

            @Override
            String readSysFsCgroupV2MemoryLimitInBytes(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                // This is the highest value that can be stored in an unsigned 64 bit number, hence too big for long
                return "18446744073709551615";
            }

            @Override
            String readSysFsCgroupV2MemoryUsageInBytes(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return "4796416";
            }

            @Override
            List<String> readCgroupV2CpuStats(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return List.of(
                    "usage_usec 364869866063",
                    "user_usec 34636",
                    "system_usec 9896",
                    "nr_periods 17992",
                    "nr_throttled 1311",
                    "throttled_usec 139298645"
                );
            }

            @Override
            String readCgroupV2CpuLimit(String controlGroup) {
                assertThat(controlGroup, equalTo("/" + hierarchy));
                return "50000 100000";
            }
        };
    }

    private static OsProbe buildStubOsProbe(final int availableCgroupsVersion, final String hierarchy, List<String> procSelfCgroupLines) {
        return buildStubOsProbe(availableCgroupsVersion, hierarchy, procSelfCgroupLines, List.of());
    }

}
