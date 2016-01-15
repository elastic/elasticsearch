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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class BulkByScrollTaskTests extends ESTestCase {
    private BulkByScrollTask task;

    public void testBasicData() {
        assertEquals(1, task.getId());
        assertEquals("test_type", task.getType());
        assertEquals("test_action", task.getAction());
    }

    public void testUnstarted() {
        assertEquals("unstarted", task.getDescription());
    }

    public void testProgress() {
        long totalHits = randomIntBetween(10, 1000);
        long created = 0;
        long updated = 0;
        long deleted = 0;
        long versionConflicts = 0;
        long noops = 0;
        int batch = 0;
        task.setTotal(totalHits);
        for (long p = 0; p < totalHits; p++) {
            String description = task.getDescription();
            assertThat(description, startsWith(p + "/" + totalHits));
            assertThat(description, containsString("created=" + created));
            assertThat(description, containsString("updated=" + updated));
            assertThat(description, containsString("deleted=" + deleted));
            assertThat(description, containsString("version_conflicts=" + versionConflicts));
            assertThat(description, containsString("batch=" + batch));
            assertThat(description, containsString("noops=" + noops));

            if (randomBoolean()) {
                created++;
                task.countCreated();
            } else if (randomBoolean()) {
                updated++;
                task.countUpdated();
            } else {
                deleted++;
                task.countDeleted();
            }

            if (rarely()) {
                versionConflicts++;
                task.countVersionConflict();
            }

            if (rarely()) {
                batch++;
                task.countBatch();
            }

            if (rarely()) {
                noops++;
                task.countNoop();
            }
        }
        String description = task.getDescription();
        assertThat(description, startsWith(totalHits + "/" + totalHits));
        assertThat(description, containsString("created=" + created));
        assertThat(description, containsString("updated=" + updated));
        assertThat(description, containsString("deleted=" + deleted));
        assertThat(description, containsString("version_conflicts=" + versionConflicts));
        assertThat(description, containsString("batch=" + batch));
        assertThat(description, containsString("noops=" + noops));
    }

    @Before
    public void createTask() {
        task = new BulkByScrollTask(1, "test_type", "test_action");
    }
}
