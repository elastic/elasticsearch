/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

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

    private FlushJobParams(boolean calcInterim, TimeRange timeRange, Long advanceTimeSeconds, Long skipTimeSeconds) {
        this.calcInterim = calcInterim;
        this.timeRange = Objects.requireNonNull(timeRange);
        this.advanceTimeSeconds = advanceTimeSeconds;
        this.skipTimeSeconds = skipTimeSeconds;
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
        if (!shouldAdvanceTime()) {
            throw new IllegalStateException();
        }
        return advanceTimeSeconds;
    }

    public long getSkipTime() {
        if (!shouldSkipTime()) {
            throw new IllegalStateException();
        }
        return skipTimeSeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlushJobParams that = (FlushJobParams) o;
        return calcInterim == that.calcInterim &&
                Objects.equals(timeRange, that.timeRange) &&
                Objects.equals(advanceTimeSeconds, that.advanceTimeSeconds) &&
                Objects.equals(skipTimeSeconds, that.skipTimeSeconds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(calcInterim, timeRange, advanceTimeSeconds, skipTimeSeconds);
    }

    public static class Builder {
        private boolean calcInterim = false;
        private TimeRange timeRange = TimeRange.builder().build();
        private String advanceTime;
        private String skipTime;

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

        public FlushJobParams build() {
            checkValidFlushArgumentsCombination();
            Long advanceTimeSeconds = parseTimeParam("advance_time", advanceTime);
            Long skipTimeSeconds = parseTimeParam("skip_time", skipTime);
            if (skipTimeSeconds != null && advanceTimeSeconds != null && advanceTimeSeconds <= skipTimeSeconds) {
                throw ExceptionsHelper.badRequestException("advance_time [" + advanceTime + "] must be later than skip_time ["
                        + skipTime + "]");
            }
            return new FlushJobParams(calcInterim, timeRange, advanceTimeSeconds, skipTimeSeconds);
        }

        private void checkValidFlushArgumentsCombination() {
            if (!calcInterim) {
                checkFlushParamIsEmpty(TimeRange.START_PARAM, timeRange.getStart());
                checkFlushParamIsEmpty(TimeRange.END_PARAM, timeRange.getEnd());
            } else if (!isValidTimeRange(timeRange)) {
                String msg = Messages.getMessage(Messages.REST_INVALID_FLUSH_PARAMS_MISSING, "start");
                throw new IllegalArgumentException(msg);
            }
        }

        private Long parseTimeParam(String name, String value) {
            if (Strings.isNullOrEmpty(value)) {
                return null;
            }
            return paramToEpochIfValidOrThrow(name, value) / TimeRange.MILLISECONDS_IN_SECOND;
        }

        private long paramToEpochIfValidOrThrow(String paramName, String date) {
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

        private void checkFlushParamIsEmpty(String paramName, String paramValue) {
            if (!paramValue.isEmpty()) {
                String msg = Messages.getMessage(Messages.REST_INVALID_FLUSH_PARAMS_UNEXPECTED, paramName);
                throw new IllegalArgumentException(msg);
            }
        }

        private boolean isValidTimeRange(TimeRange timeRange) {
            return !timeRange.getStart().isEmpty() || (timeRange.getStart().isEmpty() && timeRange.getEnd().isEmpty());
        }
    }
}
