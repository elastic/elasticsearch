/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.os;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.util.Arrays;
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

    private final OsProbe probe = OsProbe.getInstance();

    public void testOsInfo() {
        int allocatedProcessors = randomIntBetween(1, Runtime.getRuntime().availableProcessors());
        long refreshInterval = randomBoolean() ? -1 : randomNonNegativeLong();
        OsInfo info = probe.osInfo(refreshInterval, allocatedProcessors);
        assertNotNull(info);
        assertEquals(refreshInterval, info.getRefreshInterval());
        assertEquals(Constants.OS_NAME, info.getName());
        assertEquals(Constants.OS_ARCH, info.getArch());
        assertEquals(Constants.OS_VERSION, info.getVersion());
        assertEquals(allocatedProcessors, info.getAllocatedProcessors());
        assertEquals(Runtime.getRuntime().availableProcessors(), info.getAvailableProcessors());
    }

    public void testOsStats() {
        OsStats stats = probe.osStats();
        assertNotNull(stats);
        assertThat(stats.getTimestamp(), greaterThan(0L));
        assertThat(stats.getCpu().getPercent(), anyOf(equalTo((short) -1),
                is(both(greaterThanOrEqualTo((short) 0)).and(lessThanOrEqualTo((short) 100)))));
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
            assertThat(stats.getSwap().getFree().getBytes(), greaterThan(0L));
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
                assertThat(stats.getCgroup().getCpuAcctUsageNanos(), greaterThan(0L));
                assertThat(stats.getCgroup().getCpuCfsQuotaMicros(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
                assertThat(stats.getCgroup().getCpuCfsPeriodMicros(), greaterThanOrEqualTo(0L));
                assertThat(stats.getCgroup().getCpuStat().getNumberOfElapsedPeriods(), greaterThanOrEqualTo(0L));
                assertThat(stats.getCgroup().getCpuStat().getNumberOfTimesThrottled(), greaterThanOrEqualTo(0L));
                assertThat(stats.getCgroup().getCpuStat().getTimeThrottledNanos(), greaterThanOrEqualTo(0L));
                // These could be null if transported from a node running an older version, but shouldn't be null on the current node
                assertThat(stats.getCgroup().getMemoryControlGroup(), notNullValue());
                assertThat(stats.getCgroup().getMemoryLimitInBytes(), notNullValue());
                assertThat(new BigInteger(stats.getCgroup().getMemoryLimitInBytes()), greaterThan(BigInteger.ZERO));
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
        assumeTrue("test runs on Linux only", Constants.LINUX);

        final boolean areCgroupStatsAvailable = randomBoolean();
        final String hierarchy = randomAlphaOfLength(16);

        final OsProbe probe = new OsProbe() {

            @Override
            List<String> readProcSelfCgroup() {
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
                        "0::/cgroup2");
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
                return Arrays.asList(
                    "nr_periods 17992",
                    "nr_throttled 1311",
                    "throttled_time 139298645489");
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
                return areCgroupStatsAvailable;
            }

        };

        final OsStats.Cgroup cgroup = probe.osStats().getCgroup();

        if (areCgroupStatsAvailable) {
            assertNotNull(cgroup);
            assertThat(cgroup.getCpuAcctControlGroup(), equalTo("/" + hierarchy));
            assertThat(cgroup.getCpuAcctUsageNanos(), equalTo(364869866063112L));
            assertThat(cgroup.getCpuControlGroup(), equalTo("/" + hierarchy));
            assertThat(cgroup.getCpuCfsPeriodMicros(), equalTo(100000L));
            assertThat(cgroup.getCpuCfsQuotaMicros(), equalTo(50000L));
            assertThat(cgroup.getCpuStat().getNumberOfElapsedPeriods(), equalTo(17992L));
            assertThat(cgroup.getCpuStat().getNumberOfTimesThrottled(), equalTo(1311L));
            assertThat(cgroup.getCpuStat().getTimeThrottledNanos(), equalTo(139298645489L));
            assertThat(cgroup.getMemoryLimitInBytes(), equalTo("18446744073709551615"));
            assertThat(cgroup.getMemoryUsageInBytes(), equalTo("4796416"));
        } else {
            assertNull(cgroup);
        }
    }

}
