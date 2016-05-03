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

package org.elasticsearch.monitor.fs;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;

public class FsProbeTests extends ESTestCase {
    public void testFsInfo() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsProbe probe = new FsProbe(Settings.EMPTY, env);

            FsInfo stats = probe.stats();
            assertValidStatsData(stats);
        }
    }

    private void assertValidStatsData(FsInfo stats) {
        assertNotNull(stats);
        assertThat(stats.getTimestamp(), greaterThan(0L));

        FsInfo.Path total = stats.getTotal();
        assertNotNull(total);
        assertThat(total.total, greaterThan(0L));
        assertThat(total.free, greaterThan(0L));
        assertThat(total.available, greaterThan(0L));

        for (FsInfo.Path path : stats) {
            assertNotNull(path);
            assertThat(path.getPath(), not(isEmptyOrNullString()));
            assertThat(path.getMount(), not(isEmptyOrNullString()));
            assertThat(path.getType(), not(isEmptyOrNullString()));
            assertThat(path.total, greaterThan(0L));
            assertThat(path.free, greaterThan(0L));
            assertThat(path.available, greaterThan(0L));
        }
    }

    public void testNoMatchQuota() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            FsProbe subject = new FsProbe(Settings.EMPTY, nodeEnvironment);

            subject.setQuota(PathUtils.get("/noMatchPath/"), new ByteSizeValue(100));
            assertValidStatsData(subject.stats());
        }
    }

    public void testLimit() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment()) {
            FsProbe subject = new FsProbe(Settings.EMPTY, nodeEnvironment);

            Path limitedPath = nodeEnvironment.nodeDataPaths()[0].getParent();
            ByteSizeValue limit = new ByteSizeValue(100);
            subject.setQuota(limitedPath, limit);
            FsInfo stats = subject.stats();
            assertValidStatsData(stats);
            boolean found = false;
            for (FsInfo.Path path : stats) {
                String pathString = path.getPath();
                if (pathString.startsWith(limitedPath.toString())) {
                    found = true;
                    assertEquals(new ByteSizeValue(100), path.getTotal());

                }
            }
            assertTrue("Expected quotaPath to be found in stat", found);
        }
    }
}
