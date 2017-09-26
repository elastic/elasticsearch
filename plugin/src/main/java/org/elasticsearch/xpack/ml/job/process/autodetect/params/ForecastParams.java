/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.util.Objects;

public class ForecastParams {

    private final long endTime;
    private final long id;

    private ForecastParams(long id, long endTime) {
        this.id = id;
        this.endTime = endTime;
    }

    /**
     * The forecast end time in seconds from the epoch
     * @return The end time in seconds from the epoch
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * The forecast id
     * 
     * @return The forecast Id
     */
    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, endTime);
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
        return Objects.equals(id, other.id) && Objects.equals(endTime, other.endTime);
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long endTimeEpochSecs;
        private long startTime;
        private long forecastId;

        private Builder() {
            startTime = System.currentTimeMillis();
            endTimeEpochSecs = tomorrow(startTime);
            forecastId = generateId();
        }

        static long tomorrow(long now) {
            return (now / 1000) + (60 * 60 * 24);
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

        public ForecastParams build() {
            return new ForecastParams(forecastId, endTimeEpochSecs);
        }
    }
}

