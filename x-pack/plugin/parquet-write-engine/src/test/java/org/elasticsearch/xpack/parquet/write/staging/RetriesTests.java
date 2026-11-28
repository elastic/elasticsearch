/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.staging;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class RetriesTests extends ESTestCase {

    /** Records the waits instead of sleeping. */
    private final List<Long> waits = new ArrayList<>();

    private final Retries.Pause pause = waits::add;

    public void testAnAttemptThatWorksIsNotRepeatedAndNothingWaits() throws Exception {
        final int[] calls = new int[1];
        final String result = Retries.run(3, 250, attempt -> {
            calls[0]++;
            return "done";
        }, pause);
        assertThat(result, equalTo("done"));
        assertThat(calls[0], equalTo(1));
        assertThat(waits, empty());
    }

    public void testAFailureThatPassesIsRetriedWithWaitsThatGrowByFourTimes() throws Exception {
        final String result = Retries.run(4, 250, attempt -> {
            if (attempt < 3) {
                if (randomBoolean()) {
                    throw new IOException("attempt " + attempt);
                }
                throw new IllegalStateException("attempt " + attempt);
            }
            return "attempt " + attempt;
        }, pause);
        assertThat(result, equalTo("attempt 3"));
        assertThat(waits, contains(250L, 1000L, 4000L));
    }

    public void testWhenEveryAttemptFailsTheLastExceptionIsThrownWithTheEarlierOnesSuppressed() {
        final IOException e = expectThrows(
            IOException.class,
            () -> Retries.run(3, 250, attempt -> { throw new IOException("attempt " + attempt); }, pause)
        );
        assertThat(e.getMessage(), equalTo("attempt 2"));
        assertThat(e.getSuppressed(), arrayWithSize(2));
        assertThat(waits, contains(250L, 1000L)); // no wait after the last attempt
    }

    public void testARuntimeExceptionIsThrownAsItIs() {
        final IllegalStateException failure = new IllegalStateException("no");
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> Retries.run(1, 250, attempt -> { throw failure; }, pause)
        );
        assertThat(e, sameInstance(failure));
        assertThat(waits, empty());
    }

    public void testAnErrorIsNotRetried() {
        final int[] calls = new int[1];
        expectThrows(NoClassDefFoundError.class, () -> Retries.run(3, 250, attempt -> {
            calls[0]++;
            throw new NoClassDefFoundError("a/Class");
        }, pause));
        assertThat(calls[0], equalTo(1));
        assertThat(waits, empty());
    }

    public void testAnInterruptionEndsTheWaitingAndKeepsTheFlag() {
        final int[] calls = new int[1];
        final IOException e = expectThrows(IOException.class, () -> Retries.run(3, 250, attempt -> {
            calls[0]++;
            throw new IOException("attempt " + attempt);
        }, millis -> { throw new InterruptedException(); }));
        try {
            assertThat(calls[0], equalTo(1));
            assertThat(e.getSuppressed()[0], instanceOf(InterruptedException.class));
            assertTrue("the interrupted flag stays set", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // clear the flag, so that the test framework does not see it
        }
    }

    public void testAtLeastOneAttemptIsNeeded() {
        expectThrows(IllegalArgumentException.class, () -> Retries.run(0, 250, attempt -> "x", pause));
    }
}
