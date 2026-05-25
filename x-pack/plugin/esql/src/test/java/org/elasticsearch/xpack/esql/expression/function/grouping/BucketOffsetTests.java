/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.time.Duration;
import java.time.Instant;

public class BucketOffsetTests extends ESTestCase {

    public void testDurationSpanUsesOffset() {
        Bucket bucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T00:00:00Z")),
            Literal.timeDuration(Source.EMPTY, Duration.ofHours(1)),
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER,
            Duration.ofMinutes(30).toMillis(),
            Rounding.RoundingConvention.UP
        );

        long value = Instant.parse("2024-01-01T01:20:00Z").toEpochMilli();
        long rounded = bucket.getDateRounding(FoldContext.small(), null, null).round(value);
        assertEquals(Instant.parse("2024-01-01T01:30:00Z").toEpochMilli(), rounded);
    }

    public void testAutoSpanIgnoresOffset() {
        // The auto-span (numeric bucket count) path does not support offset yet.
        // TSTEP only uses the duration span path where offset is applied.
        Bucket bucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T00:00:00Z")),
            Literal.integer(Source.EMPTY, 4),
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T00:00:00Z")),
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T04:00:00Z")),
            ConfigurationAware.CONFIGURATION_MARKER,
            Duration.ofMinutes(30).toMillis(),
            Rounding.RoundingConvention.UP
        );

        long value = Instant.parse("2024-01-01T01:20:00Z").toEpochMilli();
        long rounded = bucket.getDateRounding(FoldContext.small(), null, null).round(value);
        assertEquals(Instant.parse("2024-01-01T02:00:00Z").toEpochMilli(), rounded);
    }
}
