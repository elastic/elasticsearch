/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.process;

import org.apache.lucene.util.Constants;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ProcessProbeTests extends ESTestCase {

    public void testProcessInfo() {
        long refreshInterval = randomNonNegativeLong();
        ProcessInfo info = ProcessProbe.processInfo(refreshInterval);
        assertNotNull(info);
        assertEquals(refreshInterval, info.getRefreshInterval());
        assertEquals(jvmInfo().pid(), info.getId());
        assertEquals(BootstrapInfo.isMemoryLocked(), info.isMlockall());
    }

    public void testProcessStats() {
        ProcessStats stats = ProcessProbe.processStats();
        assertNotNull(stats);
        assertThat(stats.getTimestamp(), greaterThan(0L));

        if (Constants.WINDOWS) {
            // Open/Max files descriptors are not supported on Windows platforms
            assertThat(stats.getOpenFileDescriptors(), equalTo(-1L));
            assertThat(stats.getMaxFileDescriptors(), equalTo(-1L));
        } else {
            assertThat(stats.getOpenFileDescriptors(), greaterThan(0L));
            assertThat(stats.getMaxFileDescriptors(), greaterThan(0L));
        }

        ProcessStats.Cpu cpu = stats.getCpu();
        assertNotNull(cpu);

        // CPU percent can be negative if the system recent cpu usage is not available
        assertThat(cpu.getPercent(), anyOf(lessThan((short) 0), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100))));

        // CPU time can return -1 if the platform does not support this operation, let's see which platforms fail
        assertThat(cpu.getTotal().millis(), greaterThan(0L));

        ProcessStats.Mem mem = stats.getMem();
        assertNotNull(mem);
        // Committed total virtual memory can return -1 if not supported, let's see which platforms fail
        assertThat(mem.getTotalVirtual().getBytes(), greaterThan(0L));
    }
}
