/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;

/**
 * Decorator that counts the form each page of a string column is handed over in. A profiled reader wraps the sink
 * of each {@link StringColumnReader#readBlock} call in it. A reader without a recorder passes the sink of the caller
 * directly.
 *
 * <p>Only the two calls that begin a page are wrapped. The values of a value page go through the
 * {@link StringBlockSink.Values} of the caller, so counting costs nothing per value. A page is counted when it
 * begins, before the sink of the caller receives it.
 *
 * <p>The counts are of the pages handed to the sink, whatever the consumer does with them. A read of one document at
 * a time is not a page and is not counted.
 */
final class ProfiledStringBlockSink implements StringBlockSink {

    static final CounterMetric STRING_PAGES_AS_ORDINALS = CounterMetric.of(
        "ColumNAR",
        "pages",
        "as_ordinals",
        Metric.Unit.COUNT,
        "Pages of a string column that were handed over as ordinals into a dictionary of their own. Counted when the page begins."
    );

    static final CounterMetric STRING_PAGES_AS_VALUES = CounterMetric.of(
        "ColumNAR",
        "pages",
        "as_values",
        Metric.Unit.COUNT,
        "Pages of a string column that were handed over as their values, for pages that repeat too little for ordinals. "
            + "Empty pages are not counted. Counted when the page begins."
    );

    static final CounterMetric STRING_VALUES_IN_ORDINAL_PAGES = CounterMetric.of(
        "ColumNAR",
        "pages",
        "values_in_ordinal_pages",
        Metric.Unit.COUNT,
        "Values carried by the pages that were handed over as ordinals."
    );

    static final CounterMetric STRING_VALUES_IN_VALUE_PAGES = CounterMetric.of(
        "ColumNAR",
        "pages",
        "values_in_value_pages",
        Metric.Unit.COUNT,
        "Values carried by the pages that were handed over as values."
    );

    /** The counters of one reader. The reader gets them when it is built, so wrapping a page looks nothing up. */
    static final class Counters {
        private final Counter ordinalPages;
        private final Counter valuePages;
        private final Counter ordinalValues;
        private final Counter valueValues;

        Counters(MetricRecorder metrics) {
            this.ordinalPages = metrics.counter(STRING_PAGES_AS_ORDINALS);
            this.valuePages = metrics.counter(STRING_PAGES_AS_VALUES);
            this.ordinalValues = metrics.counter(STRING_VALUES_IN_ORDINAL_PAGES);
            this.valueValues = metrics.counter(STRING_VALUES_IN_VALUE_PAGES);
        }
    }

    private final StringBlockSink delegate;
    private final Counters counters;

    ProfiledStringBlockSink(StringBlockSink delegate, Counters counters) {
        this.delegate = delegate;
        this.counters = counters;
    }

    @Override
    public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int dictionarySize) {
        counters.ordinalPages.add(1);
        counters.ordinalValues.add(valueCount);
        delegate.appendOrdinals(ordinals, valueCount, valueCounts, docCount, dictionary, dictionarySize);
    }

    @Override
    public Values values(int valueCount, int[] valueCounts, int docCount) {
        if (docCount > 0) {
            counters.valuePages.add(1);
            counters.valueValues.add(valueCount);
        }
        return delegate.values(valueCount, valueCounts, docCount);
    }
}
