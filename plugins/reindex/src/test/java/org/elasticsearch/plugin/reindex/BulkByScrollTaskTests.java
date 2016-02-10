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

public class BulkByScrollTaskTests extends ESTestCase {
    private BulkByScrollTask task;

    @Before
    public void createTask() {
        task = new BulkByScrollTask(1, "test_type", "test_action", "test");
    }

    public void testBasicData() {
        assertEquals(1, task.getId());
        assertEquals("test_type", task.getType());
        assertEquals("test_action", task.getAction());
    }

    public void testProgress() {
        long created = 0;
        long updated = 0;
        long deleted = 0;
        long versionConflicts = 0;
        long noops = 0;
        int batch = 0;
        BulkByScrollTask.Status status = task.getStatus();
        assertEquals(0, status.getTotal());
        assertEquals(created, status.getCreated());
        assertEquals(updated, status.getUpdated());
        assertEquals(deleted, status.getDeleted());
        assertEquals(versionConflicts, status.getVersionConflicts());
        assertEquals(batch, status.getBatches());
        assertEquals(noops, status.getNoops());

        long totalHits = randomIntBetween(10, 1000);
        task.setTotal(totalHits);
        for (long p = 0; p < totalHits; p++) {
            status = task.getStatus();
            assertEquals(totalHits, status.getTotal());
            assertEquals(created, status.getCreated());
            assertEquals(updated, status.getUpdated());
            assertEquals(deleted, status.getDeleted());
            assertEquals(versionConflicts, status.getVersionConflicts());
            assertEquals(batch, status.getBatches());
            assertEquals(noops, status.getNoops());

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
        status = task.getStatus();
        assertEquals(totalHits, status.getTotal());
        assertEquals(created, status.getCreated());
        assertEquals(updated, status.getUpdated());
        assertEquals(deleted, status.getDeleted());
        assertEquals(versionConflicts, status.getVersionConflicts());
        assertEquals(batch, status.getBatches());
        assertEquals(noops, status.getNoops());
    }

    public void testStatusHatesNegatives() {
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(-1, 0, 0, 0, 0, 0, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, -1, 0, 0, 0, 0, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, 0, -1, 0, 0, 0, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, 0, 0, -1, 0, 0, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, 0, 0, 0, -1, 0, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, 0, 0, 0, 0, -1, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, -1, 0));
        expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, -1));
    }
}
