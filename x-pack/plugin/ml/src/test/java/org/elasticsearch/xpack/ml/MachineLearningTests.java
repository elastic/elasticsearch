/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MachineLearningTests extends ESTestCase {

    public void testMaxOpenWorkersSetting_givenDefault() {
        int maxOpenWorkers = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(Settings.EMPTY);
        assertEquals(20, maxOpenWorkers);
    }

    public void testMaxOpenWorkersSetting_givenSetting() {
        Settings.Builder settings = Settings.builder();
        settings.put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), 7);
        int maxOpenWorkers = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings.build());
        assertEquals(7, maxOpenWorkers);
    }

    public void testMaxMachineMemoryPercent_givenDefault() {
        int maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(Settings.EMPTY);
        assertEquals(30, maxMachineMemoryPercent);
    }

    public void testMaxMachineMemoryPercent_givenValidSetting() {
        Settings.Builder settings = Settings.builder();
        int expectedMaxMachineMemoryPercent = randomIntBetween(5, 200);
        settings.put(MachineLearning.MAX_MACHINE_MEMORY_PERCENT.getKey(), expectedMaxMachineMemoryPercent);
        int maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings.build());
        assertEquals(expectedMaxMachineMemoryPercent, maxMachineMemoryPercent);
    }

    public void testMaxMachineMemoryPercent_givenInvalidSetting() {
        Settings.Builder settings = Settings.builder();
        int invalidMaxMachineMemoryPercent = randomFrom(4, 201);
        settings.put(MachineLearning.MAX_MACHINE_MEMORY_PERCENT.getKey(), invalidMaxMachineMemoryPercent);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings.build()));
        assertThat(e.getMessage(), startsWith("Failed to parse value [" + invalidMaxMachineMemoryPercent
            + "] for setting [xpack.ml.max_machine_memory_percent] must be"));
    }

    public void testNoAttributes_givenNoClash() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            builder.put("xpack.ml.max_open_jobs", randomIntBetween(9, 12));
        }
        builder.put("node.attr.foo", "abc");
        builder.put("node.attr.ml.bar", "def");
        MachineLearning machineLearning = createMachineLearning(builder.put("path.home", createTempDir()).build());
        assertNotNull(machineLearning.additionalSettings());
    }

    public void testNoAttributes_givenSameAndMlEnabled() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            int maxOpenJobs = randomIntBetween(5, 15);
            builder.put("xpack.ml.max_open_jobs", maxOpenJobs);
        }
        MachineLearning machineLearning = createMachineLearning(builder.put("path.home", createTempDir()).build());
        assertNotNull(machineLearning.additionalSettings());
    }

    public void testNoAttributes_givenClash() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("node.attr.ml.enabled", randomBoolean());
        } else {
            builder.put("node.attr.ml.max_open_jobs", randomIntBetween(13, 15));
        }
        MachineLearning machineLearning = createMachineLearning(builder.put("path.home", createTempDir()).build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, machineLearning::additionalSettings);
        assertThat(e.getMessage(), startsWith("Directly setting [node.attr.ml."));
        assertThat(e.getMessage(), containsString("] is not permitted - " +
                "it is reserved for machine learning. If your intention was to customize machine learning, set the [xpack.ml."));
    }

    public void testMachineMemory_givenStatsFailure() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(0, 0));
        assertEquals(0L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenNoCgroup() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        assertEquals(10_737_418_240L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenCgroupNullLimit() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        when(stats.getCgroup()).thenReturn(new OsStats.Cgroup("a", 1, "b", 2, 3,
                new OsStats.Cgroup.CpuStat(4, 5, 6), null, null, null));
        assertEquals(10_737_418_240L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenCgroupNoLimit() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        when(stats.getCgroup()).thenReturn(new OsStats.Cgroup("a", 1, "b", 2, 3,
                new OsStats.Cgroup.CpuStat(4, 5, 6), "c", "18446744073709551615", "4796416"));
        assertEquals(10_737_418_240L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenCgroupLowLimit() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        when(stats.getCgroup()).thenReturn(new OsStats.Cgroup("a", 1, "b", 2, 3,
                new OsStats.Cgroup.CpuStat(4, 5, 6), "c", "7516192768", "4796416"));
        assertEquals(7_516_192_768L, MachineLearning.machineMemoryFromStats(stats));
    }

    private MachineLearning createMachineLearning(Settings settings) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);

        return new MachineLearning(settings, null){
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
    }
}
