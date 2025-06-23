/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

/**
 * A shared interface for aggregations that parse and use "interval" parameters.
 *
 * Provides definitions for the new fixed and calendar intervals, and deprecated
 * defintions for the old interval/dateHisto interval parameters
 */
public interface DateIntervalConsumer<T> {
    T calendarInterval(DateHistogramInterval interval);

    T fixedInterval(DateHistogramInterval interval);
}
