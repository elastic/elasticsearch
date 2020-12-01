/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;

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

    public boolean isAfter(long timestampMs) {
        long jobLatestResultTime = latestFinalBucketTimeMs == null ? 0 : latestFinalBucketTimeMs;
        long jobLatestRecordTime = latestRecordTimeMs == null ? 0 : latestRecordTimeMs;
        return Math.max(jobLatestResultTime, jobLatestRecordTime) > timestampMs;
    }

    public boolean isAfterModelSnapshot(ModelSnapshot modelSnapshot) {
        assert modelSnapshot != null;
        long jobLatestResultTime = latestFinalBucketTimeMs == null ? 0 : latestFinalBucketTimeMs;
        long jobLatestRecordTime = latestRecordTimeMs == null ? 0 : latestRecordTimeMs;
        long modelLatestResultTime = modelSnapshot.getLatestResultTimeStamp() == null ?
                0 : modelSnapshot.getLatestResultTimeStamp().getTime();
        long modelLatestRecordTime = modelSnapshot.getLatestRecordTimeStamp() == null ?
                0 : modelSnapshot.getLatestRecordTimeStamp().getTime();
        return jobLatestResultTime > modelLatestResultTime || jobLatestRecordTime > modelLatestRecordTime;
    }
}
