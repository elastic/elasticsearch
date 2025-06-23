/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Objects;

public class FlushJobParams {

    /**
     * Whether interim results should be calculated
     */
    private final boolean calcInterim;

    /**
     * The time range for which interim results should be calculated
     */
    private final TimeRange timeRange;

    /**
     * The epoch (seconds) to advance time to
     */
    private final Long advanceTimeSeconds;

    /**
     * The epoch (seconds) to skip time to
     */
    private final Long skipTimeSeconds;

    /**
     * Should the flush request wait for normalization or not.
     */
    private final boolean waitForNormalization;

    /**
     * Should the flush request trigger a refresh or not.
     */
    private final boolean refreshRequired;

    private FlushJobParams(
        boolean calcInterim,
        TimeRange timeRange,
        Long advanceTimeSeconds,
        Long skipTimeSeconds,
        boolean waitForNormalization,
        boolean refreshRequired
    ) {
        this.calcInterim = calcInterim;
        this.timeRange = Objects.requireNonNull(timeRange);
        this.advanceTimeSeconds = advanceTimeSeconds;
        this.skipTimeSeconds = skipTimeSeconds;
        this.waitForNormalization = waitForNormalization;
        this.refreshRequired = refreshRequired;
    }

    public boolean shouldCalculateInterim() {
        return calcInterim;
    }

    public boolean shouldAdvanceTime() {
        return advanceTimeSeconds != null;
    }

    public boolean shouldSkipTime() {
        return skipTimeSeconds != null;
    }

    public String getStart() {
        return timeRange.getStart();
    }

    public String getEnd() {
        return timeRange.getEnd();
    }

    public long getAdvanceTime() {
        if (shouldAdvanceTime() == false) {
            throw new IllegalStateException();
        }
        return advanceTimeSeconds;
    }

    public long getSkipTime() {
        if (shouldSkipTime() == false) {
            throw new IllegalStateException();
        }
        return skipTimeSeconds;
    }

    public boolean isWaitForNormalization() {
        return waitForNormalization;
    }

    public boolean isRefreshRequired() {
        return refreshRequired;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlushJobParams that = (FlushJobParams) o;
        return calcInterim == that.calcInterim
            && Objects.equals(timeRange, that.timeRange)
            && Objects.equals(advanceTimeSeconds, that.advanceTimeSeconds)
            && Objects.equals(skipTimeSeconds, that.skipTimeSeconds)
            && waitForNormalization == that.waitForNormalization
            && refreshRequired == that.refreshRequired;
    }

    @Override
    public int hashCode() {
        return Objects.hash(calcInterim, timeRange, advanceTimeSeconds, skipTimeSeconds, waitForNormalization, refreshRequired);
    }

    public static class Builder {
        private boolean calcInterim = false;
        private TimeRange timeRange = TimeRange.builder().build();
        private String advanceTime;
        private String skipTime;
        private boolean waitForNormalization = true;
        private boolean refreshRequired = true;

        public Builder calcInterim(boolean value) {
            calcInterim = value;
            return this;
        }

        public Builder forTimeRange(TimeRange timeRange) {
            this.timeRange = timeRange;
            return this;
        }

        public Builder advanceTime(String timestamp) {
            advanceTime = ExceptionsHelper.requireNonNull(timestamp, "advance_time");
            return this;
        }

        public Builder skipTime(String timestamp) {
            skipTime = ExceptionsHelper.requireNonNull(timestamp, "skip_time");
            return this;
        }

        public Builder waitForNormalization(boolean waitForNormalization) {
            this.waitForNormalization = waitForNormalization;
            return this;
        }

        public Builder refreshRequired(boolean refreshRequired) {
            this.refreshRequired = refreshRequired;
            return this;
        }

        public FlushJobParams build() {
            checkValidFlushArgumentsCombination();
            Long advanceTimeSeconds = parseTimeParam("advance_time", advanceTime);
            Long skipTimeSeconds = parseTimeParam("skip_time", skipTime);
            if (skipTimeSeconds != null && advanceTimeSeconds != null && advanceTimeSeconds <= skipTimeSeconds) {
                throw ExceptionsHelper.badRequestException(
                    "advance_time [" + advanceTime + "] must be later than skip_time [" + skipTime + "]"
                );
            }
            return new FlushJobParams(calcInterim, timeRange, advanceTimeSeconds, skipTimeSeconds, waitForNormalization, refreshRequired);
        }

        private void checkValidFlushArgumentsCombination() {
            if (calcInterim == false) {
                checkFlushParamIsEmpty(TimeRange.START_PARAM, timeRange.getStart());
                checkFlushParamIsEmpty(TimeRange.END_PARAM, timeRange.getEnd());
            } else if (isValidTimeRange(timeRange) == false) {
                String msg = Messages.getMessage(Messages.REST_INVALID_FLUSH_PARAMS_MISSING, "start");
                throw new IllegalArgumentException(msg);
            }
        }

        private static Long parseTimeParam(String name, String value) {
            if (Strings.isNullOrEmpty(value)) {
                return null;
            }
            return paramToEpochIfValidOrThrow(name, value) / TimeRange.MILLISECONDS_IN_SECOND;
        }

        private static long paramToEpochIfValidOrThrow(String paramName, String date) {
            if (TimeRange.NOW.equals(date)) {
                return System.currentTimeMillis();
            }
            long epoch = 0;
            if (date.isEmpty() == false) {
                epoch = TimeUtils.dateStringToEpoch(date);
                if (epoch < 0) {
                    String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName, date);
                    throw new ElasticsearchParseException(msg);
                }
            }
            return epoch;
        }

        private static void checkFlushParamIsEmpty(String paramName, String paramValue) {
            if (paramValue.isEmpty() == false) {
                String msg = Messages.getMessage(Messages.REST_INVALID_FLUSH_PARAMS_UNEXPECTED, paramName);
                throw new IllegalArgumentException(msg);
            }
        }

        private static boolean isValidTimeRange(TimeRange timeRange) {
            return timeRange.getStart().isEmpty() == false || (timeRange.getStart().isEmpty() && timeRange.getEnd().isEmpty());
        }
    }
}
