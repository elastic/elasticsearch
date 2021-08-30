/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

/**
 * A shared interface for aggregations that parse and use "interval" parameters.
 *
 * Provides definitions for the new fixed and calendar intervals, and deprecated
 * defintions for the old interval/dateHisto interval parameters
 */
public interface DateIntervalConsumer<T> {
    @Deprecated
    T interval(long interval);

    @Deprecated
    T dateHistogramInterval(DateHistogramInterval dateHistogramInterval);

    T calendarInterval(DateHistogramInterval interval);

    T fixedInterval(DateHistogramInterval interval);

    @Deprecated
    long interval();

    @Deprecated
    DateHistogramInterval dateHistogramInterval();
}
