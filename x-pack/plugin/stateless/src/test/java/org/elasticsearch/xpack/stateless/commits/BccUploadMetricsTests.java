/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.stateless.commits.BccUploadMetrics.bccSizeBucket;
import static org.elasticsearch.xpack.stateless.commits.BccUploadMetrics.bccTimestampSpanMinutes;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BccUploadMetricsTests extends ESTestCase {

    public void testBccSizeBucketBoundaries() {
        assertThat(bccSizeBucket(1), equalTo("<=16MiB"));
        assertThat(bccSizeBucket(ByteSizeUnit.MB.toBytes(16)), equalTo("<=16MiB"));
        assertThat(bccSizeBucket(ByteSizeUnit.MB.toBytes(16) + 1), equalTo("<=64MiB"));
        assertThat(bccSizeBucket(ByteSizeUnit.MB.toBytes(64)), equalTo("<=64MiB"));
        assertThat(bccSizeBucket(ByteSizeUnit.MB.toBytes(64) + 1), equalTo("<=256MiB"));
        assertThat(bccSizeBucket(ByteSizeUnit.MB.toBytes(256)), equalTo("<=256MiB"));
        assertThat(bccSizeBucket(ByteSizeUnit.MB.toBytes(256) + 1), equalTo(">256MiB"));

        assertThat(bccSizeBucket(randomLongBetween(1, ByteSizeUnit.MB.toBytes(16))), equalTo("<=16MiB"));
        assertThat(bccSizeBucket(randomLongBetween(ByteSizeUnit.MB.toBytes(16) + 1, ByteSizeUnit.MB.toBytes(64))), equalTo("<=64MiB"));
        assertThat(bccSizeBucket(randomLongBetween(ByteSizeUnit.MB.toBytes(64) + 1, ByteSizeUnit.MB.toBytes(256))), equalTo("<=256MiB"));
        assertThat(bccSizeBucket(randomLongBetween(ByteSizeUnit.MB.toBytes(256) + 1, Long.MAX_VALUE)), equalTo(">256MiB"));
    }

    public void testBccTimestampSpanMinutesEmptyWhenNoTimestamps() {
        assertThat(bccTimestampSpanMinutes(Collections.emptyIterator()), equalTo(OptionalDouble.empty()));
        assertThat(
            bccTimestampSpanMinutes(Arrays.<StatelessCompoundCommit.TimestampFieldValueRange>asList(null, null).iterator()),
            equalTo(OptionalDouble.empty())
        );
    }

    public void testBccTimestampSpanMinutesAggregatesAcrossCommits() {
        final long tenYearsMillis = TimeUnit.DAYS.toMillis(3650);
        final long oneYearMillis = TimeUnit.DAYS.toMillis(365);

        // single range: span is exactly (max - min) / 60000
        {
            final long min = randomLongBetween(0, tenYearsMillis);
            final long max = min + randomLongBetween(0, oneYearMillis);
            final OptionalDouble span = bccTimestampSpanMinutes(List.of(new StatelessCompoundCommit.TimestampFieldValueRange(min, max)).iterator());
            assertThat(span.isPresent(), is(true));
            assertThat(span.getAsDouble(), closeTo((double) (max - min) / 60_000d, 1e-9));
        }

        // zero-width range -> 0.0
        {
            final long ts = randomLongBetween(0, tenYearsMillis);
            final OptionalDouble span = bccTimestampSpanMinutes(List.of(new StatelessCompoundCommit.TimestampFieldValueRange(ts, ts)).iterator());
            assertThat(span.isPresent(), is(true));
            assertThat(span.getAsDouble(), closeTo(0.0, 1e-9));
        }

        // multiple ranges aggregate to the overall [min, max]; interspersed nulls must be ignored
        {
            final int n = randomIntBetween(1, 6);
            final List<StatelessCompoundCommit.TimestampFieldValueRange> ranges = new ArrayList<>();
            long expectedMin = Long.MAX_VALUE;
            long expectedMax = Long.MIN_VALUE;
            for (int i = 0; i < n; i++) {
                final long min = randomLongBetween(0, tenYearsMillis);
                final long max = min + randomLongBetween(0, oneYearMillis);
                expectedMin = Math.min(expectedMin, min);
                expectedMax = Math.max(expectedMax, max);
                ranges.add(new StatelessCompoundCommit.TimestampFieldValueRange(min, max));
            }
            ranges.add(null);
            ranges.add(null);
            Collections.shuffle(ranges, random());

            final OptionalDouble span = bccTimestampSpanMinutes(ranges.iterator());
            assertThat(span.isPresent(), is(true));
            assertThat(span.getAsDouble(), closeTo((double) (expectedMax - expectedMin) / 60_000d, 1e-9));
        }
    }

    public void testBccTimestampSpanMinutesDoesNotThrowOnHugeSpan() {
        final OptionalDouble span = bccTimestampSpanMinutes(
            List.of(new StatelessCompoundCommit.TimestampFieldValueRange(Long.MIN_VALUE + 1, Long.MAX_VALUE)).iterator()
        );
        assertThat(span.isPresent(), is(true));
        assertThat(span.getAsDouble(), closeTo(((double) Long.MAX_VALUE - (double) (Long.MIN_VALUE + 1)) / 60_000d, 1.0));
    }
}
