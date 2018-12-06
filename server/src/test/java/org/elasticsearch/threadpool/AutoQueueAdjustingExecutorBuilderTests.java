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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.CoreMatchers.containsString;

public class AutoQueueAdjustingExecutorBuilderTests extends ESThreadPoolTestCase {

    public void testValidatingMinMaxSettings() {
        Settings settings = Settings.builder()
                .put("thread_pool.test.min_queue_size", randomIntBetween(30, 100))
                .put("thread_pool.test.max_queue_size", randomIntBetween(1,25))
                .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 10);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Failed to parse value"));
        }

        settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 10)
            .put("thread_pool.test.max_queue_size", 9)
            .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [10] for setting [thread_pool.test.min_queue_size] must be <= 9");
        }

        settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 11)
            .put("thread_pool.test.max_queue_size", 10)
            .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [11] for setting [thread_pool.test.min_queue_size] must be <= 10");
        }

        settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 101)
            .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 100, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [101] for setting [thread_pool.test.min_queue_size] must be <= 100");
        }

        settings = Settings.builder()
            .put("thread_pool.test.max_queue_size", 99)
            .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 100, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [100] for setting [thread_pool.test.min_queue_size] must be <= 99");
        }
    }

    public void testSetLowerSettings() {
        Settings settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 10)
            .put("thread_pool.test.max_queue_size", 10)
            .build();
        AutoQueueAdjustingExecutorBuilder test = new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 1000, 1000, 1000, 2000);
        AutoQueueAdjustingExecutorBuilder.AutoExecutorSettings s = test.getSettings(settings);
        assertEquals(10, s.maxQueueSize);
        assertEquals(10, s.minQueueSize);
    }

    public void testSetHigherSettings() {
        Settings settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 2000)
            .put("thread_pool.test.max_queue_size", 3000)
            .build();
        AutoQueueAdjustingExecutorBuilder test = new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 1000, 1000, 1000, 2000);
        AutoQueueAdjustingExecutorBuilder.AutoExecutorSettings s = test.getSettings(settings);
        assertEquals(3000, s.maxQueueSize);
        assertEquals(2000, s.minQueueSize);
    }

}
