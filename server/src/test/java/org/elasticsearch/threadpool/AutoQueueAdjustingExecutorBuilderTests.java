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

    public void testValidatingMinMaxSettings() throws Exception {
        Settings settings = Settings.builder()
                .put("thread_pool.search.min_queue_size", randomIntBetween(30, 100))
                .put("thread_pool.search.max_queue_size", randomIntBetween(1,25))
                .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 10);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Failed to parse value"));
        }
    }

}
