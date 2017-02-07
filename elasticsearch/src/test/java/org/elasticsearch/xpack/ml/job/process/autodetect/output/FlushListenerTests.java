/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlushListenerTests extends ESTestCase {

    public void testAcknowledgeFlush() throws Exception {
        FlushListener listener = new FlushListener();
        AtomicBoolean bool = new AtomicBoolean();
        new Thread(() -> {
            boolean result = listener.waitForFlush("_id", Duration.ofMillis(10000));
            bool.set(result);
        }).start();
        assertBusy(() -> assertTrue(listener.awaitingFlushed.containsKey("_id")));
        assertFalse(bool.get());
        listener.acknowledgeFlush("_id");
        assertBusy(() -> assertTrue(bool.get()));
        assertEquals(1, listener.awaitingFlushed.size());

        listener.clear("_id");
        assertEquals(0, listener.awaitingFlushed.size());
    }

    public void testClear() throws Exception {
        FlushListener listener = new FlushListener();

        int numWaits = 9;
        List<AtomicBoolean> bools = new ArrayList<>(numWaits);
        for (int i = 0; i < numWaits; i++) {
            int id = i;
            AtomicBoolean bool = new AtomicBoolean();
            bools.add(bool);
            new Thread(() -> {
                boolean result = listener.waitForFlush(String.valueOf(id), Duration.ofMillis(10000));
                bool.set(result);
            }).start();
        }
        assertBusy(() -> assertEquals(numWaits, listener.awaitingFlushed.size()));
        for (AtomicBoolean bool : bools) {
            assertFalse(bool.get());
        }
        assertFalse(listener.cleared.get());
        listener.clear();
        for (AtomicBoolean bool : bools) {
            assertBusy(() -> assertTrue(bool.get()));
        }
        assertTrue(listener.awaitingFlushed.isEmpty());
        assertTrue(listener.cleared.get());
    }

}
