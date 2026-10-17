/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.os;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CgroupV2ProbeTests extends ESTestCase {

    public void testParseCpuPressure() {
        final var pressure = CgroupV2Probe.parseCpuPressure(
            List.of("some avg10=0.12 avg60=0.34 avg300=0.56 total=123456789", "full avg10=0.00 avg60=0.00 avg300=0.00 total=0")
        );
        assertThat(pressure, equalTo(new CgroupV2Probe.CpuPressure(123456789L)));
    }

    public void testParseCpuPressureMalformed() {
        assertThat(CgroupV2Probe.parseCpuPressure(List.of()), nullValue());
        assertThat(CgroupV2Probe.parseCpuPressure(List.of("full avg10=0.00 total=5")), nullValue());
        assertThat(CgroupV2Probe.parseCpuPressure(List.of("some avg10=0.00")), nullValue());
        assertThat(CgroupV2Probe.parseCpuPressure(List.of("some avg10=0.00 total=abc")), nullValue());
        assertThat(CgroupV2Probe.parseCpuPressure(List.of("some avg10=0.00 total=-5")), nullValue());
        assertThat(CgroupV2Probe.parseCpuPressure(List.of("", "some")), nullValue());
    }

    public void testParseCpuThrottling() {
        final var throttling = CgroupV2Probe.parseCpuThrottling(
            List.of(
                "usage_usec 1000",
                "user_usec 600",
                "system_usec 400",
                "nr_periods 50",
                "nr_throttled 7",
                "throttled_usec 12345",
                "nr_bursts 0",
                "burst_usec 0"
            )
        );
        assertThat(throttling, equalTo(new CgroupV2Probe.CpuThrottling(12345L, 7L)));
    }

    public void testParseCpuThrottlingWithoutCpuController() {
        // a control group without the cpu controller only has the usage fields and cannot be throttled
        assertThat(
            CgroupV2Probe.parseCpuThrottling(List.of("usage_usec 1000", "user_usec 600", "system_usec 400")),
            equalTo(new CgroupV2Probe.CpuThrottling(0L, 0L))
        );
    }

    public void testParseCpuThrottlingMalformed() {
        assertThat(CgroupV2Probe.parseCpuThrottling(List.of()), nullValue());
        assertThat(CgroupV2Probe.parseCpuThrottling(List.of("garbage", "")), nullValue());
        assertThat(CgroupV2Probe.parseCpuThrottling(List.of("usage_usec 1", "throttled_usec abc")), nullValue());
        assertThat(CgroupV2Probe.parseCpuThrottling(List.of("usage_usec 1", "nr_throttled -3")), nullValue());
        // other malformed lines are skipped
        assertThat(
            CgroupV2Probe.parseCpuThrottling(List.of("usage_usec 1", "too many fields here", "throttled_usec 5")),
            equalTo(new CgroupV2Probe.CpuThrottling(5L, 0L))
        );
    }

    private static class FakeOsProbe extends OsProbe {
        String controlGroup = "/kubepods/pod1";
        List<String> cpuPressure = List.of("some avg10=0.00 avg60=0.00 avg300=0.00 total=9");
        List<String> cpuStat = List.of("usage_usec 1", "nr_throttled 2", "throttled_usec 3");
        boolean failReads;

        @Override
        String getCgroupV2ControlGroup() {
            return controlGroup;
        }

        @Override
        List<String> readCgroupV2CpuPressure(String controlGroup) throws IOException {
            return read(controlGroup, cpuPressure);
        }

        @Override
        List<String> readCgroupV2CpuStats(String controlGroup) throws IOException {
            return read(controlGroup, cpuStat);
        }

        private List<String> read(String controlGroup, List<String> lines) throws IOException {
            assertThat(controlGroup, equalTo(this.controlGroup));
            if (failReads) {
                throw new IOException("unreadable");
            }
            return lines;
        }
    }

    private static OsProbe osProbeWithProcSelfCgroup(List<String> lines) {
        return new OsProbe() {
            @Override
            List<String> readProcSelfCgroup() {
                return lines;
            }
        };
    }

    public void testControlGroupLookup() throws IOException {
        assertThat(osProbeWithProcSelfCgroup(List.of("0::/kubepods/pod1")).getCgroupV2ControlGroup(), equalTo("/kubepods/pod1"));
        // cgroups v1, and a hybrid hierarchy, are not v2
        assertThat(osProbeWithProcSelfCgroup(List.of("12:cpu,cpuacct:/a", "3:memory:/b")).getCgroupV2ControlGroup(), nullValue());
        assertThat(osProbeWithProcSelfCgroup(List.of("1:name=systemd:/x", "0::/y")).getCgroupV2ControlGroup(), nullValue());
    }

    public void testReadsFromTheControlGroup() {
        final var probe = new CgroupV2Probe(new FakeOsProbe());
        assertThat(probe.getCpuPressure(), equalTo(new CgroupV2Probe.CpuPressure(9L)));
        assertThat(probe.getCpuThrottling(), equalTo(new CgroupV2Probe.CpuThrottling(3L, 2L)));
    }

    public void testUnavailableWhenNotInCgroupV2() {
        final var os = new FakeOsProbe();
        os.controlGroup = null;
        final var probe = new CgroupV2Probe(os);
        assertThat(probe.getCpuPressure(), nullValue());
        assertThat(probe.getCpuThrottling(), nullValue());
    }

    public void testUnavailableWhenFilesCannotBeRead() {
        final var os = new FakeOsProbe();
        os.failReads = true;
        final var probe = new CgroupV2Probe(os);
        assertThat(probe.getCpuPressure(), nullValue());
        assertThat(probe.getCpuThrottling(), nullValue());
        // and again, after the unavailability was logged once
        assertThat(probe.getCpuPressure(), nullValue());
        os.failReads = false;
        assertThat(probe.getCpuPressure(), equalTo(new CgroupV2Probe.CpuPressure(9L)));
    }

    public void testUnavailableWhenFilesCannotBeParsed() {
        final var os = new FakeOsProbe();
        os.cpuPressure = List.of();
        os.cpuStat = List.of("garbage");
        final var probe = new CgroupV2Probe(os);
        assertThat(probe.getCpuPressure(), nullValue());
        assertThat(probe.getCpuThrottling(), nullValue());
    }

    public void testRealProbeDoesNotThrow() {
        // whatever this machine is, reading must never throw
        CgroupV2Probe.getInstance().getCpuPressure();
        CgroupV2Probe.getInstance().getCpuThrottling();
    }
}
