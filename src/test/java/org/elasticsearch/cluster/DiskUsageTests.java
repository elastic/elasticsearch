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

package org.elasticsearch.cluster;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class DiskUsageTests extends ElasticsearchTestCase {

    @Test
    public void diskUsageCalcTest() {
        DiskUsage du = new DiskUsage("node1", 100, 40);
        assertThat(du.getFreeDiskAsPercentage(), equalTo(40.0));
        assertThat(du.getFreeBytes(), equalTo(40L));
        assertThat(du.getUsedBytes(), equalTo(60L));
        assertThat(du.getTotalBytes(), equalTo(100L));

    }

    @Test
    public void randomDiskUsageTest() {
        int iters = scaledRandomIntBetween(1000, 10000);
        for (int i = 1; i < iters; i++) {
            long total = between(Integer.MIN_VALUE, Integer.MAX_VALUE);
            long free = between(Integer.MIN_VALUE, Integer.MAX_VALUE);
            if (free > total || total <= 0) {
                try {
                    new DiskUsage("random", total, free);
                    fail("should never reach this");
                } catch (IllegalStateException e) {
                }
            } else {
                DiskUsage du = new DiskUsage("random", total, free);
                assertThat(du.getFreeBytes(), equalTo(free));
                assertThat(du.getTotalBytes(), equalTo(total));
                assertThat(du.getUsedBytes(), equalTo(total - free));
                assertThat(du.getFreeDiskAsPercentage(), equalTo(100.0 * ((double)free / total)));
            }
        }
    }
}
