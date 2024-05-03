/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Objects;

public class TimeRange {

    public static final String START_PARAM = "start";
    public static final String END_PARAM = "end";
    public static final String NOW = "now";
    public static final int MILLISECONDS_IN_SECOND = 1000;

    private final Long start;
    private final Long end;

    private TimeRange(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() {
        return start == null ? "" : String.valueOf(start);
    }

    public String getEnd() {
        return end == null ? "" : String.valueOf(end);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRange timeRange = (TimeRange) o;
        return Objects.equals(start, timeRange.start) && Objects.equals(end, timeRange.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    public static class Builder {

        private String start = "";
        private String end = "";

        private Builder() {}

        public Builder startTime(String start) {
            this.start = ExceptionsHelper.requireNonNull(start, "start");
            return this;
        }

        public Builder endTime(String end) {
            this.end = ExceptionsHelper.requireNonNull(end, "end");
            return this;
        }

        /**
         * Create a new TimeRange instance after validating the start and end params.
         * Throws {@link ElasticsearchStatusException} if the validation fails
         * @return The time range
         */
        public TimeRange build() {
            return createTimeRange(start, end);
        }

        private static TimeRange createTimeRange(String start, String end) {
            Long epochStart = null;
            Long epochEnd = null;
            if (start.isEmpty() == false) {
                epochStart = paramToEpochIfValidOrThrow(START_PARAM, start) / MILLISECONDS_IN_SECOND;
                epochEnd = paramToEpochIfValidOrThrow(END_PARAM, end) / MILLISECONDS_IN_SECOND;
                if (end.isEmpty() || epochEnd.equals(epochStart)) {
                    epochEnd = epochStart + 1;
                }
                if (epochEnd < epochStart) {
                    String msg = Messages.getMessage(Messages.REST_START_AFTER_END, end, start);
                    throw new IllegalArgumentException(msg);
                }
            } else {
                if (end.isEmpty() == false) {
                    epochEnd = paramToEpochIfValidOrThrow(END_PARAM, end) / MILLISECONDS_IN_SECOND;
                }
            }
            return new TimeRange(epochStart, epochEnd);
        }

        /**
         * Returns epoch milli seconds
         */
        private static long paramToEpochIfValidOrThrow(String paramName, String date) {
            if (NOW.equals(date)) {
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
    }
}
