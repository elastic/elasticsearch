/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public final class EventsIndex {
    private static final String PREFIX = "profiling-events";
    private static final String ALL_EVENTS = PREFIX + "-all";

    private static final int SAMPLING_FACTOR = 5;

    private static final int MIN_EXPONENT = 1;

    private static final int MAX_EXPONENT = 11;

    public static final EventsIndex FULL_INDEX = new EventsIndex(ALL_EVENTS, 1, 0);

    // Start with counting the results in the index down-sampled by 5^6.
    // That is in the middle of our down-sampled indexes.
    public static final EventsIndex MEDIUM_DOWNSAMPLED = fromFactorAndExponent(SAMPLING_FACTOR, 6);

    private final String name;

    private final int samplingFactor;

    private final int exponent;

    private EventsIndex(String name, int samplingFactor, int exponent) {
        this.name = name;
        this.samplingFactor = samplingFactor;
        this.exponent = exponent;
    }

    public String getName() {
        return name;
    }

    public int getExponent() {
        return exponent;
    }

    public double getSampleRate() {
        return Math.pow(1.0d / samplingFactor, exponent);
    }

    public EventsIndex getResampledIndex(long targetSampleSize, long currentSampleSize) {
        return EventsIndex.getSampledIndex(targetSampleSize, currentSampleSize, this.getExponent());
    }

    // Return the index that has between targetSampleSize..targetSampleSize*samplingFactor entries.
    // The starting point is the number of entries from the profiling-events-5pow<initialExp> index.
    private static EventsIndex getSampledIndex(long targetSampleSize, long sampleCountFromInitialExp, int initialExp) {
        if (sampleCountFromInitialExp == 0) {
            return FULL_INDEX;
        }
        int exp = initialExp - (int) Math.round(
            Math.log((targetSampleSize * SAMPLING_FACTOR) / (double) sampleCountFromInitialExp) / Math.log(SAMPLING_FACTOR)
        ) + 1;

        if (exp < MIN_EXPONENT) {
            return FULL_INDEX;
        }
        if (exp > MAX_EXPONENT) {
            exp = MAX_EXPONENT;
        }
        return fromFactorAndExponent(SAMPLING_FACTOR, exp);
    }

    private static EventsIndex fromFactorAndExponent(int factor, int exp) {
        return new EventsIndex(indexName(factor, exp), factor, exp);
    }

    private static String indexName(int factor, int exp) {
        return String.format(Locale.ROOT, "%s-%dpow%02d", PREFIX, factor, exp);
    }

    public static Collection<String> indexNames() {
        Set<String> names = new HashSet<>();
        names.add(EventsIndex.ALL_EVENTS);
        for (int exp = MIN_EXPONENT; exp <= MAX_EXPONENT; exp++) {
            names.add(indexName(SAMPLING_FACTOR, exp));
        }
        return Collections.unmodifiableSet(names);
    }
}
