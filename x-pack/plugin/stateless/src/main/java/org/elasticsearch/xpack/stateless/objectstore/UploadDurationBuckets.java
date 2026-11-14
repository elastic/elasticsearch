/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import java.util.List;

/// Histogram bucket boundaries for the duration of uploads to the object store.
public final class UploadDurationBuckets {

    // 1 to 1_000_000 milliseconds (= ~16 min)
    public static final List<Long> BOUNDARIES_IN_MILLIS = List.of(
        1L,
        5L,
        10L,
        25L,
        50L,
        100L,
        250L,
        500L,
        1_000L,
        2_500L,
        5_000L,
        10_000L,
        30_000L,
        60_000L,
        120_000L,
        300_000L,
        600_000L,
        900_000L,
        1_000_000L
    );

    private UploadDurationBuckets() {}
}
