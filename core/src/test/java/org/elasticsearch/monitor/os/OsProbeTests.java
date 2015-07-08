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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class OsProbeTests extends ElasticsearchTestCase {

    OsProbe probe = OsProbe.getInstance();

    @Test
    public void testOsInfo() {
        OsInfo info = probe.osInfo();
        assertNotNull(info);
        assertThat(info.getRefreshInterval(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        assertThat(info.getName(), equalTo(Constants.OS_NAME));
        assertThat(info.getArch(), equalTo(Constants.OS_ARCH));
        assertThat(info.getVersion(), equalTo(Constants.OS_VERSION));
        assertThat(info.getAvailableProcessors(), equalTo(Runtime.getRuntime().availableProcessors()));
    }

    @Test
    public void testOsStats() {
        OsStats stats = probe.osStats();
        assertNotNull(stats);
        assertThat(stats.getTimestamp(), greaterThan(0L));
        assertThat(stats.getLoadAverage(), anyOf(equalTo((double) -1), greaterThanOrEqualTo((double) 0)));

        assertNotNull(stats.getMem());
        assertThat(stats.getMem().getTotal().bytes(), anyOf(equalTo(-1L), greaterThan(0L)));
        assertThat(stats.getMem().getFree().bytes(), anyOf(equalTo(-1L), greaterThan(0L)));
        assertThat(stats.getMem().getFreePercent(), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100)));
        assertThat(stats.getMem().getUsed().bytes(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        assertThat(stats.getMem().getUsedPercent(), allOf(greaterThanOrEqualTo((short) 0), lessThanOrEqualTo((short) 100)));

        assertNotNull(stats.getSwap());
        assertThat(stats.getSwap().getTotal().bytes(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        assertThat(stats.getSwap().getFree().bytes(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
        assertThat(stats.getSwap().getUsed().bytes(), anyOf(equalTo(-1L), greaterThanOrEqualTo(0L)));
    }
}
