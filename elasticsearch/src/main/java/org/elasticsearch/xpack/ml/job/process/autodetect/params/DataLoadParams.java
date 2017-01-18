/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.xpack.ml.job.DataDescription;

import java.util.Objects;
import java.util.Optional;

public class DataLoadParams {
    private final TimeRange resetTimeRange;
    private final boolean ignoreDowntime;
    private final Optional<DataDescription> dataDescription;

    public DataLoadParams(TimeRange resetTimeRange, boolean ignoreDowntime, Optional<DataDescription> dataDescription) {
        this.resetTimeRange = Objects.requireNonNull(resetTimeRange);
        this.ignoreDowntime = ignoreDowntime;
        this.dataDescription = Objects.requireNonNull(dataDescription);
    }

    public boolean isResettingBuckets() {
        return !getStart().isEmpty();
    }

    public String getStart() {
        return resetTimeRange.getStart();
    }

    public String getEnd() {
        return resetTimeRange.getEnd();
    }

    public boolean isIgnoreDowntime() {
        return ignoreDowntime;
    }

    public Optional<DataDescription> getDataDescription() {
        return dataDescription;
    }
}

