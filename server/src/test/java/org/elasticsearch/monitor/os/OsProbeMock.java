/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.os;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * A mock implementation of {@link OsProbe} that overrides all methods that would normally read from the filesystem. The {@code set*()}
 * methods in this class can be called to specify the exact values to be returned by this mock.
 */
final class OsProbeMock extends OsProbe {

    private final String hierarchy;
    private int availableCgroupsVersion = 0;
    private List<String> procSelfCgroupLines = List.of();
    private List<String> procMeminfoLines = List.of();
    private List<String> cgroupCpuMax = List.of("50000 100000");

    /**
     * Creates a new instance of a mock {@link OsProbe}.
     *
     * @param hierarchy a mock value used to generate a cgroup hierarchy.
     */
    OsProbeMock(String hierarchy) {
        this.hierarchy = hierarchy;
    }

    /**
     * Set the cgroups version.
     *
     * @param availableCgroupsVersion what version of cgroups are available, 1 or 2, or 0 for no cgroups. Normally OsProbe establishes this
     *                                for itself.
     * @return {@code this}.
     */
    OsProbeMock setAvailableCgroupsVersion(int availableCgroupsVersion) {
        this.availableCgroupsVersion = availableCgroupsVersion;
        return this;
    }

    /**
     * Sets the contents of {@code /proc/self/cgroup}.
     *
     * @param procSelfCgroupLines the lines that will be used as the content of {@code /proc/self/cgroup}.
     * @return {@code this}.
     */
    OsProbeMock setProcSelfCgroupLines(List<String> procSelfCgroupLines) {
        this.procSelfCgroupLines = procSelfCgroupLines;
        return this;
    }

    /**
     * Sets the contents of {@code /proc/meminfo}.
     *
     * @param procMeminfoLines lines that will be used as the content of {@code /proc/meminfo}.
     * @return {@code this}.
     */
    OsProbeMock setProcMeminfoLines(List<String> procMeminfoLines) {
        this.procMeminfoLines = procMeminfoLines;
        return this;
    }

    /**
     * Sets the contents of {@code /sys/fs/cgroup/${hierarchy}/cpu.max}.
     *
     * @param cgroupCpuMax a single line content of {@code /sys/fs/cgroup/${hierarchy}/}, where {@code ${hierarchy}} is the value passed to
     *                    the {@link #OsProbeMock(String)} constructor.
     * @return {@code this}.
     */
    OsProbeMock setCgroupCpuMax(List<String> cgroupCpuMax) {
        this.cgroupCpuMax = cgroupCpuMax;
        return this;
    }

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
    List<String> readCgroupV2CpuLimit(String controlGroup) {
        assertThat(controlGroup, equalTo("/" + hierarchy));
        return cgroupCpuMax;
    }
}
