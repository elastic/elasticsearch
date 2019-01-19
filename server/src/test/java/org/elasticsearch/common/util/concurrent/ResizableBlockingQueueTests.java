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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ResizableBlockingQueueTests extends ESTestCase {

    public void testAdjustCapacity() throws Exception {
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(),
                        100);

        assertThat(queue.capacity(), equalTo(100));
        // Queue size already equal to desired capacity
        queue.adjustCapacity(100, 25, 1, 1000);
        assertThat(queue.capacity(), equalTo(100));
        // Not worth adjusting
        queue.adjustCapacity(99, 25, 1, 1000);
        assertThat(queue.capacity(), equalTo(100));
        // Not worth adjusting
        queue.adjustCapacity(75, 25, 1, 1000);
        assertThat(queue.capacity(), equalTo(100));
        queue.adjustCapacity(74, 25, 1, 1000);
        assertThat(queue.capacity(), equalTo(75));
        queue.adjustCapacity(1000000, 25, 1, 1000);
        assertThat(queue.capacity(), equalTo(100));
        queue.adjustCapacity(1, 25, 80, 1000);
        assertThat(queue.capacity(), equalTo(80));
        queue.adjustCapacity(1000000, 25, 80, 100);
        assertThat(queue.capacity(), equalTo(100));
    }
}
