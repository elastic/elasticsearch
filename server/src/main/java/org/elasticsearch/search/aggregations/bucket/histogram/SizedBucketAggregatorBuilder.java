/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import java.util.concurrent.TimeUnit;

/**
 * An aggregator capable of reporting bucket sizes in milliseconds. Used by RateAggregator for calendar-based buckets.
 */
public interface SizedBucketAggregatorBuilder {
    double calendarDivider(TimeUnit timeUnit);
}
