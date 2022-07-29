/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FlushListenerTests extends ESTestCase {

    public void testAcknowledgeFlush() throws Exception {
        FlushListener listener = new FlushListener();
        AtomicReference<FlushAcknowledgement> flushAcknowledgementHolder = new AtomicReference<>();
        new Thread(() -> {
            try {
                FlushAcknowledgement flushAcknowledgement = listener.waitForFlush("_id", Duration.ofMillis(10000));
                flushAcknowledgementHolder.set(flushAcknowledgement);
            } catch (InterruptedException _ex) {
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                fail("unexpected exception " + ex.getMessage());
            }
        }).start();
        assertBusy(() -> assertTrue(listener.awaitingFlushed.containsKey("_id")));
        assertNull(flushAcknowledgementHolder.get());
        FlushAcknowledgement flushAcknowledgement = new FlushAcknowledgement("_id", 12345678L);
        listener.acknowledgeFlush(flushAcknowledgement, null);
        assertBusy(() -> assertNotNull(flushAcknowledgementHolder.get()));
        assertEquals(1, listener.awaitingFlushed.size());

        listener.clear("_id");
        assertEquals(0, listener.awaitingFlushed.size());
    }

    public void testAcknowledgeFlushFailure() throws Exception {
        FlushListener listener = new FlushListener();
        AtomicReference<Exception> flushExceptionHolder = new AtomicReference<>();
        new Thread(() -> {
            try {
                listener.waitForFlush("_id", Duration.ofMillis(10000));
                fail("Expected exception to throw.");
            } catch (InterruptedException _ex) {
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                flushExceptionHolder.set(ex);
            }
        }).start();
        assertBusy(() -> assertTrue(listener.awaitingFlushed.containsKey("_id")));
        assertNull(flushExceptionHolder.get());
        FlushAcknowledgement flushAcknowledgement = new FlushAcknowledgement("_id", Instant.ofEpochMilli(12345678L));
        listener.acknowledgeFlush(flushAcknowledgement, new Exception("BOOM"));
        assertBusy(() -> {
            assertNotNull(flushExceptionHolder.get());
            assertThat(flushExceptionHolder.get().getMessage(), equalTo("BOOM"));
        });
        assertEquals(1, listener.awaitingFlushed.size());

        listener.clear("_id");
        assertEquals(0, listener.awaitingFlushed.size());
    }

    public void testClear() throws Exception {
        FlushListener listener = new FlushListener();

        int numWaits = 9;
        List<AtomicReference<FlushAcknowledgement>> flushAcknowledgementHolders = new ArrayList<>(numWaits);
        for (int i = 0; i < numWaits; i++) {
            int id = i;
            AtomicReference<FlushAcknowledgement> flushAcknowledgementHolder = new AtomicReference<>();
            flushAcknowledgementHolders.add(flushAcknowledgementHolder);
            new Thread(() -> {
                try {
                    FlushAcknowledgement flushAcknowledgement = listener.waitForFlush(String.valueOf(id), Duration.ofMillis(10000));
                    flushAcknowledgementHolder.set(flushAcknowledgement);
                } catch (InterruptedException _ex) {
                    Thread.currentThread().interrupt();
                } catch (Exception ex) {
                    fail("unexpected exception " + ex.getMessage());
                }
            }).start();
        }
        assertBusy(() -> assertEquals(numWaits, listener.awaitingFlushed.size()));
        assertThat(flushAcknowledgementHolders.stream().map(f -> f.get()).filter(f -> f != null).findAny().isPresent(), is(false));
        assertFalse(listener.onClear.hasRun());

        listener.clear();

        for (AtomicReference<FlushAcknowledgement> f : flushAcknowledgementHolders) {
            assertBusy(() -> assertNotNull(f.get()));
        }
        assertTrue(listener.awaitingFlushed.isEmpty());
        assertTrue(listener.onClear.hasRun());
    }
}
