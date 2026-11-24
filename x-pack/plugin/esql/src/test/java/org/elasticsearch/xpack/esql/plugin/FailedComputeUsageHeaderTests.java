/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class FailedComputeUsageHeaderTests extends ESTestCase {

    private static DriverCompletionInfo usage(long base) {
        return new DriverCompletionInfo(
            base,
            base + 1,
            base + 2,
            base + 3,
            base + 4,
            base + 5,
            base + 6,
            base + 7,
            List.of(),
            List.of(),
            Map.of(),
            false,
            false,
            Set.of()
        );
    }

    private static void assertSameNumbers(DriverCompletionInfo actual, DriverCompletionInfo expected) {
        assertThat(actual.documentsFound(), equalTo(expected.documentsFound()));
        assertThat(actual.valuesLoaded(), equalTo(expected.valuesLoaded()));
        assertThat(actual.rowsEmitted(), equalTo(expected.rowsEmitted()));
        assertThat(actual.bytesRead(), equalTo(expected.bytesRead()));
        assertThat(actual.readNanos(), equalTo(expected.readNanos()));
        assertThat(actual.readCpuNanos(), equalTo(expected.readCpuNanos()));
        assertThat(actual.cpuNanos(), equalTo(expected.cpuNanos()));
        assertThat(actual.driverNanos(), equalTo(expected.driverNanos()));
    }

    public void testRoundTrip() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DriverCompletionInfo written = usage(randomLongBetween(1, 1_000_000_000L));
        FailedComputeUsageHeader.write(threadContext, written);
        assertSameNumbers(FailedComputeUsageHeader.take(threadContext), written);
    }

    public void testTakeRemovesTheHeader() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        FailedComputeUsageHeader.write(threadContext, usage(10));
        FailedComputeUsageHeader.take(threadContext);
        assertFalse(threadContext.getResponseHeaders().containsKey(FailedComputeUsageHeader.NAME));
        assertThat(FailedComputeUsageHeader.take(threadContext), sameInstance(DriverCompletionInfo.EMPTY));
    }

    public void testNothingIsWrittenForNoUsage() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        FailedComputeUsageHeader.write(threadContext, DriverCompletionInfo.EMPTY);
        assertTrue(threadContext.getResponseHeaders().isEmpty());
    }

    public void testSeveralValuesAreAddedUp() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        FailedComputeUsageHeader.write(threadContext, usage(10));
        FailedComputeUsageHeader.write(threadContext, usage(100));
        DriverCompletionInfo taken = FailedComputeUsageHeader.take(threadContext);
        assertThat(taken.bytesRead(), equalTo(13L + 103L));
        assertThat(taken.cpuNanos(), equalTo(16L + 106L));
    }

    public void testValuesThatCannotBeReadAreIgnoredAndStillRemoved() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.addResponseHeader(FailedComputeUsageHeader.NAME, "2:1,2,3,4,5,6,7,8");
        threadContext.addResponseHeader(FailedComputeUsageHeader.NAME, "1:1,2,3");
        threadContext.addResponseHeader(FailedComputeUsageHeader.NAME, "1:1,2,3,4,5,6,7,eight");
        FailedComputeUsageHeader.write(threadContext, usage(10));
        DriverCompletionInfo taken = FailedComputeUsageHeader.take(threadContext);
        // only the well formed value counts
        assertSameNumbers(taken, usage(10));
        assertFalse(threadContext.getResponseHeaders().containsKey(FailedComputeUsageHeader.NAME));
    }

    /**
     * The header carries the numeric counters of {@link DriverCompletionInfo} one by one. A counter added to it has to be
     * added to the header too, or the usage of a failed compute silently misses it.
     */
    public void testHeaderCarriesAllTheCountersOfTheCompletionInfo() {
        long counters = Arrays.stream(DriverCompletionInfo.class.getRecordComponents()).filter(c -> c.getType() == long.class).count();
        assertThat(counters, equalTo((long) FailedComputeUsageHeader.FIELDS));
    }
}
