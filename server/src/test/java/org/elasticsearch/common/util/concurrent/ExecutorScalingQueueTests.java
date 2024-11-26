/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

public class ExecutorScalingQueueTests extends ESTestCase {

    public void testPut() {
        var queue = new EsExecutors.ExecutorScalingQueue<>();
        queue.put(new Object());
        assertEquals(queue.size(), 1);
    }

    public void testAdd() {
        var queue = new EsExecutors.ExecutorScalingQueue<>();
        assertTrue(queue.add(new Object()));
        assertEquals(queue.size(), 1);
    }

    public void testTimedOffer() {
        var queue = new EsExecutors.ExecutorScalingQueue<>();
        assertTrue(queue.offer(new Object(), 60, TimeUnit.SECONDS));
        assertEquals(queue.size(), 1);
    }

}
