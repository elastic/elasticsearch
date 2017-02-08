/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.util.Objects;

public class InterimResultsParams {
    private final boolean calcInterim;
    private final TimeRange timeRange;
    private final Long advanceTimeSeconds;

    private InterimResultsParams(boolean calcInterim, TimeRange timeRange, Long advanceTimeSeconds) {
        this.calcInterim = calcInterim;
        this.timeRange = Objects.requireNonNull(timeRange);
        this.advanceTimeSeconds = advanceTimeSeconds;
    }

    public boolean shouldCalculateInterim() {
        return calcInterim;
    }

    public boolean shouldAdvanceTime() {
        return advanceTimeSeconds != null;
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

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InterimResultsParams that = (InterimResultsParams) o;
        return calcInterim == that.calcInterim &&
                Objects.equals(timeRange, that.timeRange) &&
                Objects.equals(advanceTimeSeconds, that.advanceTimeSeconds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(calcInterim, timeRange, advanceTimeSeconds);
    }

    public static class Builder {
        private boolean calcInterim = false;
        private TimeRange timeRange;
        private String advanceTime;

        private Builder() {
            calcInterim = false;
            timeRange = TimeRange.builder().build();
            advanceTime = "";
        }

        public Builder calcInterim(boolean value) {
            calcInterim = value;
            return this;
        }

        public Builder forTimeRange(TimeRange timeRange) {
            this.timeRange = timeRange;
            return this;
        }

        public Builder advanceTime(String timestamp) {
            advanceTime = ExceptionsHelper.requireNonNull(timestamp, "advance");
            return this;
        }

        public InterimResultsParams build() {
            checkValidFlushArgumentsCombination();
            Long advanceTimeSeconds = checkAdvanceTimeParam();
            return new InterimResultsParams(calcInterim, timeRange, advanceTimeSeconds);
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

        private Long checkAdvanceTimeParam() {
            if (advanceTime != null && !advanceTime.isEmpty()) {
                return paramToEpochIfValidOrThrow("advance_time", advanceTime) / TimeRange.MILLISECONDS_IN_SECOND;
            }
            return null;
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
