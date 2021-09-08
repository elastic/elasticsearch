/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * POJO for a delete forecast request
 */
public class DeleteForecastRequest implements Validatable, ToXContentObject {

    public static final ParseField FORECAST_ID = new ParseField("forecast_id");
    public static final ParseField ALLOW_NO_FORECASTS = new ParseField("allow_no_forecasts");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final String ALL = "_all";

    public static final ConstructingObjectParser<DeleteForecastRequest, Void> PARSER =
        new ConstructingObjectParser<>("delete_forecast_request", (a) -> new DeleteForecastRequest((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareStringOrNull(
            (c, p) -> c.setForecastIds(Strings.commaDelimitedListToStringArray(p)), FORECAST_ID);
        PARSER.declareBoolean(DeleteForecastRequest::setAllowNoForecasts, ALLOW_NO_FORECASTS);
        PARSER.declareString(DeleteForecastRequest::timeout, TIMEOUT);
    }

    /**
     * Create a new {@link DeleteForecastRequest} that explicitly deletes all forecasts
     *
     * @param jobId the jobId of the Job whose forecasts to delete
     */
    public static DeleteForecastRequest deleteAllForecasts(String jobId) {
        DeleteForecastRequest request = new DeleteForecastRequest(jobId);
        request.setForecastIds(ALL);
        return request;
    }

    private final String jobId;
    private List<String> forecastIds = new ArrayList<>();
    private Boolean allowNoForecasts;
    private TimeValue timeout;

    /**
     * Create a new DeleteForecastRequest for the given Job ID
     *
     * @param jobId the jobId of the Job whose forecast(s) to delete
     */
    public DeleteForecastRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId, Job.ID.getPreferredName());
    }

    public String getJobId() {
        return jobId;
    }

    public List<String> getForecastIds() {
        return forecastIds;
    }

    /**
     * The forecast IDs to delete. Can be also be {@link DeleteForecastRequest#ALL} to explicitly delete ALL forecasts
     *
     * @param forecastIds forecast IDs to delete
     */
    public void setForecastIds(String... forecastIds) {
        setForecastIds(Arrays.asList(forecastIds));
    }

    void setForecastIds(List<String> forecastIds) {
        if (forecastIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("forecastIds must not contain null values");
        }
        this.forecastIds = new ArrayList<>(forecastIds);
    }

    public Boolean getAllowNoForecasts() {
        return allowNoForecasts;
    }

    /**
     * Sets the value of "allow_no_forecasts".
     *
     * @param allowNoForecasts when {@code true} no error is thrown when {@link DeleteForecastRequest#ALL} does not find any forecasts
     */
    public void setAllowNoForecasts(boolean allowNoForecasts) {
        this.allowNoForecasts = allowNoForecasts;
    }

    /**
     * Allows to set the timeout
     * @param timeout timeout as a string (e.g. 1s)
     */
    public void timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, this.timeout, getClass().getSimpleName() + ".timeout");
    }

    /**
     * Allows to set the timeout
     * @param timeout timeout as a {@link TimeValue}
     */
    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DeleteForecastRequest that = (DeleteForecastRequest) other;
        return Objects.equals(jobId, that.jobId) &&
            Objects.equals(forecastIds, that.forecastIds) &&
            Objects.equals(allowNoForecasts, that.allowNoForecasts) &&
            Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, forecastIds, allowNoForecasts, timeout);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (forecastIds != null) {
            builder.field(FORECAST_ID.getPreferredName(), Strings.collectionToCommaDelimitedString(forecastIds));
        }
        if (allowNoForecasts != null) {
            builder.field(ALLOW_NO_FORECASTS.getPreferredName(), allowNoForecasts);
        }
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        builder.endObject();
        return builder;
    }
}
