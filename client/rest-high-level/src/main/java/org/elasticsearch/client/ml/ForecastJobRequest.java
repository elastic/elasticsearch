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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Pojo for forecasting an existing and open Machine Learning Job
 */
public class ForecastJobRequest implements Validatable, ToXContentObject {

    public static final ParseField DURATION = new ParseField("duration");
    public static final ParseField EXPIRES_IN = new ParseField("expires_in");
    public static final ParseField MAX_MODEL_MEMORY = new ParseField("max_model_memory");

    public static final ConstructingObjectParser<ForecastJobRequest, Void> PARSER =
        new ConstructingObjectParser<>("forecast_job_request", (a) -> new ForecastJobRequest((String)a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(
            (request, val) -> request.setDuration(TimeValue.parseTimeValue(val, DURATION.getPreferredName())), DURATION);
        PARSER.declareString(
            (request, val) -> request.setExpiresIn(TimeValue.parseTimeValue(val, EXPIRES_IN.getPreferredName())), EXPIRES_IN);
        PARSER.declareField(ForecastJobRequest::setMaxModelMemory, (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ByteSizeValue.parseBytesSizeValue(p.text(), MAX_MODEL_MEMORY.getPreferredName());
            } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return new ByteSizeValue(p.longValue());
            }
            throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
        }, MAX_MODEL_MEMORY, ObjectParser.ValueType.VALUE);
    }

    private final String jobId;
    private TimeValue duration;
    private TimeValue expiresIn;
    private ByteSizeValue maxModelMemory;

    /**
     * A new forecast request
     *
     * @param jobId the non-null, existing, and opened jobId to forecast
     */
    public ForecastJobRequest(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public TimeValue getDuration() {
        return duration;
    }

    /**
     * Set the forecast duration
     *
     * A period of time that indicates how far into the future to forecast.
     * The default value is 1 day. The forecast starts at the last record that was processed.
     *
     * @param duration TimeValue for the duration of the forecast
     */
    public void setDuration(TimeValue duration) {
        this.duration = duration;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    /**
     * Set the forecast expiration
     *
     * The period of time that forecast results are retained.
     * After a forecast expires, the results are deleted. The default value is 14 days.
     * If set to a value of 0, the forecast is never automatically deleted.
     *
     * @param expiresIn TimeValue for the forecast expiration
     */
    public void setExpiresIn(TimeValue expiresIn) {
        this.expiresIn = expiresIn;
    }

    public ByteSizeValue getMaxModelMemory() {
        return maxModelMemory;
    }

    /**
     * Set the amount of memory allowed to be used by this forecast.
     *
     * If the projected forecast memory usage exceeds this amount, the forecast will spool results to disk to keep within the limits.
     * @param maxModelMemory A byte sized value less than 500MB and less than 40% of the associated job's configured memory usage.
     *                       Defaults to 20MB.
     */
    public ForecastJobRequest setMaxModelMemory(ByteSizeValue maxModelMemory) {
        this.maxModelMemory = maxModelMemory;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, duration, expiresIn, maxModelMemory);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ForecastJobRequest other = (ForecastJobRequest) obj;
        return Objects.equals(jobId, other.jobId)
            && Objects.equals(duration, other.duration)
            && Objects.equals(expiresIn, other.expiresIn)
            && Objects.equals(maxModelMemory, other.maxModelMemory);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (duration != null) {
            builder.field(DURATION.getPreferredName(), duration.getStringRep());
        }
        if (expiresIn != null) {
            builder.field(EXPIRES_IN.getPreferredName(), expiresIn.getStringRep());
        }
        if (maxModelMemory != null) {
            builder.field(MAX_MODEL_MEMORY.getPreferredName(), maxModelMemory.getStringRep());
        }
        builder.endObject();
        return builder;
    }

}
