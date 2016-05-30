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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class OsProbeTests extends ESTestCase {
    OsProbe probe = OsProbe.getInstance();

    public void testOsInfo() {
        OsInfo info = probe.osInfo();
        assertNotNull(info);
        assertThat(info.getRefreshInterval(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        assertThat(info.getName(), equalTo(Constants.OS_NAME));
        assertThat(info.getArch(), equalTo(Constants.OS_ARCH));
        assertThat(info.getVersion(), equalTo(Constants.OS_VERSION));
        assertThat(info.getAvailableProcessors(), equalTo(Runtime.getRuntime().availableProcessors()));
    }

    public void testOsStats() {
        OsStats stats = probe.osStats();
        assertNotNull(stats);
        assertThat(stats.getTimestamp(), greaterThan(0L));
        assertThat(stats.getCpu().getPercent(), anyOf(equalTo((short) -1), is(both(greaterThanOrEqualTo((short) 0)).and(lessThanOrEqualTo((short) 100)))));
        double[] loadAverage = stats.getCpu().loadAverage;
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
        } else if (Constants.FREE_BSD) {
            // five- and fifteen-minute load averages not available if linprocfs is not mounted at /compat/linux/proc
            assertNotNull(loadAverage);
            assertThat(loadAverage[0], greaterThanOrEqualTo((double) 0));
            assertThat(loadAverage[1], anyOf(equalTo((double) -1), greaterThanOrEqualTo((double) 0)));
            assertThat(loadAverage[2], anyOf(equalTo((double) -1), greaterThanOrEqualTo((double) 0)));
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
        assertThat(stats.getMem().getTotal().bytes(), greaterThan(0L));
        assertThat(stats.getMem().getFree().bytes(), greaterThan(0L));
        assertThat(stats.getMem().getFreePercent(), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100)));
        assertThat(stats.getMem().getUsed().bytes(), greaterThan(0L));
        assertThat(stats.getMem().getUsedPercent(), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100)));

        assertNotNull(stats.getSwap());
        assertNotNull(stats.getSwap().getTotal());

        long total = stats.getSwap().getTotal().bytes();
        if (total > 0) {
            assertThat(stats.getSwap().getTotal().bytes(), greaterThan(0L));
            assertThat(stats.getSwap().getFree().bytes(), greaterThan(0L));
            assertThat(stats.getSwap().getUsed().bytes(), greaterThanOrEqualTo(0L));
        } else {
            // On platforms with no swap
            assertThat(stats.getSwap().getTotal().bytes(), equalTo(0L));
            assertThat(stats.getSwap().getFree().bytes(), equalTo(0L));
            assertThat(stats.getSwap().getUsed().bytes(), equalTo(0L));
        }
    }
}
