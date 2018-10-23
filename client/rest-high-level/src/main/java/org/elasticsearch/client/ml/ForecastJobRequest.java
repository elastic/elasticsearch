/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Pojo for forecasting an existing and open Machine Learning Job
 */
public class ForecastJobRequest extends ActionRequest implements ToXContentObject {

    public static final ParseField DURATION = new ParseField("duration");
    public static final ParseField EXPIRES_IN = new ParseField("expires_in");

    public static final ConstructingObjectParser<ForecastJobRequest, Void> PARSER =
        new ConstructingObjectParser<>("forecast_job_request", (a) -> new ForecastJobRequest((String)a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(
            (request, val) -> request.setDuration(TimeValue.parseTimeValue(val, DURATION.getPreferredName())), DURATION);
        PARSER.declareString(
            (request, val) -> request.setExpiresIn(TimeValue.parseTimeValue(val, EXPIRES_IN.getPreferredName())), EXPIRES_IN);
    }

    private final String jobId;
    private TimeValue duration;
    private TimeValue expiresIn;

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

    @Override
    public int hashCode() {
        return Objects.hash(jobId, duration, expiresIn);
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
            && Objects.equals(expiresIn, other.expiresIn);
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
        builder.endObject();
        return builder;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
