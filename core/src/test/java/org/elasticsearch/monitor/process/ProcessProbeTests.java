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

package org.elasticsearch.monitor.process;

import org.elasticsearch.bootstrap.Bootstrap;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo;
import static org.hamcrest.Matchers.*;

public class ProcessProbeTests extends ElasticsearchTestCase {

    ProcessProbe probe = ProcessProbe.getInstance();

    @Test
    public void testProcessInfo() {
        ProcessInfo info = probe.processInfo();
        assertNotNull(info);
        assertThat(info.getRefreshInterval(), greaterThanOrEqualTo(0L));
        assertThat(info.getId(), equalTo(jvmInfo().pid()));
        assertThat(info.isMlockall(), equalTo(Bootstrap.isMemoryLocked()));
    }

    @Test
    public void testProcessStats() {
        ProcessStats stats = probe.processStats();
        assertNotNull(stats);

        ProcessStats.Cpu cpu = stats.getCpu();
        assertNotNull(cpu);
        assertThat(cpu.getPercent(), greaterThanOrEqualTo((short) 0));
        assertThat(cpu.total, anyOf(equalTo(-1L), greaterThan(0L)));

        ProcessStats.Mem mem = stats.getMem();
        assertNotNull(mem);
        assertThat(mem.totalVirtual, anyOf(equalTo(-1L), greaterThan(0L)));
    }
}
