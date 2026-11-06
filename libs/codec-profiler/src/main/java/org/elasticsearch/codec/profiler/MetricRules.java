/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import java.util.Objects;

/** The rules every definition follows, shared by the two kinds of {@link Metric}. */
final class MetricRules {

    private MetricRules() {}

    /** Refuses a name that does not show {@code unit}, and a missing or blank description. */
    static void validate(Metric.Id id, Metric.Unit unit, String description) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(unit, "unit");
        Objects.requireNonNull(description, "description");
        final boolean showsUnit = switch (unit) {
            case COUNT -> true;
            case BYTES -> id.name().contains("bytes");
            case DOC_IDS -> id.name().contains("docid");
            case TIME_NANOS -> id.name().endsWith("_time_nanos");
        };
        if (showsUnit == false) {
            throw new IllegalArgumentException("metric name [" + id.name() + "] must show its unit [" + unit + "]");
        }
        if (description.isBlank()) {
            throw new IllegalArgumentException("metric [" + id + "] needs a description");
        }
    }

    /**
     * {@code a + b} for two counts, which are never negative, held at {@code Long.MAX_VALUE} when the sum does not fit.
     * Every count and time of this package grows this way, so a sum that reads {@code Long.MAX_VALUE} means at least
     * that much.
     */
    static long add(long a, long b) {
        assert a >= 0 && b >= 0 : "counts are never negative, got [" + a + "] and [" + b + "]";
        final long sum = a + b;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }
}
