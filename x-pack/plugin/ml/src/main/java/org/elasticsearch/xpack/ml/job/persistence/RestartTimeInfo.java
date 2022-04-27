/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.core.Nullable;

public class RestartTimeInfo {

    private final Long latestFinalBucketTimeMs;
    private final Long latestRecordTimeMs;
    private final boolean haveSeenDataPreviously;

    public RestartTimeInfo(@Nullable Long latestFinalBucketTimeMs, @Nullable Long latestRecordTimeMs, boolean haveSeenDataPreviously) {
        this.latestFinalBucketTimeMs = latestFinalBucketTimeMs;
        this.latestRecordTimeMs = latestRecordTimeMs;
        this.haveSeenDataPreviously = haveSeenDataPreviously;
    }

    @Nullable
    public Long getLatestFinalBucketTimeMs() {
        return latestFinalBucketTimeMs;
    }

    @Nullable
    public Long getLatestRecordTimeMs() {
        return latestRecordTimeMs;
    }

    public boolean haveSeenDataPreviously() {
        return haveSeenDataPreviously;
    }
}
