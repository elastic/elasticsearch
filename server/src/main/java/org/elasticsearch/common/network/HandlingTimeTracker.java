/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.network;

import org.elasticsearch.common.metrics.ExponentialBucketHistogram;

/**
 * Tracks how long message handling takes on a transport thread as a histogram with fixed buckets.
 */
public class HandlingTimeTracker extends ExponentialBucketHistogram {

    public static final int BUCKET_COUNT = 18;

    public static int[] getBucketUpperBounds() {
        return ExponentialBucketHistogram.getBucketUpperBounds(BUCKET_COUNT);
    }

    public HandlingTimeTracker() {
        super(BUCKET_COUNT);
    }
}
