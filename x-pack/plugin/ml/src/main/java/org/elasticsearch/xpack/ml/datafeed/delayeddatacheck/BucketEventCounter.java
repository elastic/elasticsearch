/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import java.util.Map;

/** Counts the events that a datafeed source has now, per bucket. */
@FunctionalInterface
public interface BucketEventCounter {

    /** Returns the current event count per bucket, keyed by bucket start time in epoch milliseconds, for {@code [start, end)}. */
    Map<Long, Long> currentBucketEventCounts(long start, long end);
}
