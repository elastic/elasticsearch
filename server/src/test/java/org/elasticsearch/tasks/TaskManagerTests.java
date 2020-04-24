/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskManagerTests extends ESTestCase {

    public void testCleanupOldBanParentMarkers() {
        AtomicLong timeInMillis = new AtomicLong(randomIntBetween(0, Integer.MAX_VALUE));
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(timeInMillis.get());
        long banRetainingIntervalInMillis = randomLongBetween(1, 5000);
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, banRetainingIntervalInMillis, Collections.emptySet());
        Map<TaskId, Long> expectedBanParents = new HashMap<>();
        TaskId taskId;
        int iters = randomIntBetween(1, 200);
        for (int i = 0; i < iters; i++) {
            taskId = new TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
            taskManager.setBan(taskId, randomAlphaOfLength(10));
            timeInMillis.addAndGet(randomInt(1000));
            expectedBanParents.values().removeIf(timestamp -> timestamp - timeInMillis.get() > banRetainingIntervalInMillis);
            expectedBanParents.put(taskId, timeInMillis.get());
            assertThat(taskManager.getBannedTaskIds(), equalTo(expectedBanParents.keySet()));
        }
    }
}
