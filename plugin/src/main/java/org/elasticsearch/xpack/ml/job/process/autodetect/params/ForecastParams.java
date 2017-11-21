/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.util.Objects;

public class ForecastParams {

    private final long endTime;
    private final long duration;
    private final long expiresIn;
    private final long forecastId;

    private ForecastParams(long forecastId, long endTime, long duration, long expiresIn) {
        this.forecastId = forecastId;
        this.endTime = endTime;
        this.duration = duration;
        this.expiresIn = expiresIn;
    }

    /**
     * The forecast end time in seconds from the epoch
     * @return The end time in seconds from the epoch
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * The forecast duration in seconds
     * @return The duration in seconds
     */
    public long getDuration() {
        return duration;
    }

    /**
     * The forecast expiration in seconds (duration added to start time)
     * @return The expiration in seconds
     */
    public long getExpiresIn() {
        return expiresIn;
    }

    /**
     * The forecast id
     * 
     * @return The forecast Id
     */
    public long getForecastId() {
        return forecastId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(forecastId, endTime, duration, expiresIn);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ForecastParams other = (ForecastParams) obj;
        return Objects.equals(forecastId, other.forecastId) && Objects.equals(endTime, other.endTime) &&
               Objects.equals(duration, other.duration) && Objects.equals(expiresIn, other.expiresIn);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long endTimeEpochSecs;
        private long durationSecs;
        private long expiresInSecs;
        private long startTime;
        private long forecastId;

        private Builder() {
            startTime = System.currentTimeMillis();
            endTimeEpochSecs = 0;
            forecastId = generateId();
            durationSecs = 0;

            // because 0 means never expire, the default is -1
            expiresInSecs = -1;
        }

        private long generateId() {
            return startTime;
        }

        public Builder endTime(String endTime, ParseField paramName) {
            DateMathParser dateMathParser = new DateMathParser(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);

            try {
                endTimeEpochSecs = dateMathParser.parse(endTime, System::currentTimeMillis) / 1000;
            } catch (Exception e) {
                String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName.getPreferredName(), endTime);
                throw new ElasticsearchParseException(msg, e);
            }

            return this;
        }

        public Builder duration(TimeValue duration) {
            durationSecs = duration.seconds();
            return this;
        }

        public Builder expiresIn(TimeValue expiresIn) {
            expiresInSecs = expiresIn.seconds();
            return this;
        }

        public ForecastParams build() {
            if (endTimeEpochSecs != 0 && durationSecs != 0) {
                throw new ElasticsearchParseException(Messages.getMessage(Messages.REST_INVALID_DURATION_AND_ENDTIME));
            }

            return new ForecastParams(forecastId, endTimeEpochSecs, durationSecs, expiresInSecs);
        }
    }
}

