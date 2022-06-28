/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ResizableBlockingQueueTests extends ESTestCase {

    public void testAdjustCapacity() throws Exception {
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

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
