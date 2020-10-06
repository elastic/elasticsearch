/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;

public class DeleteForecastAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteForecastAction INSTANCE = new DeleteForecastAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/forecast/delete";

    private DeleteForecastAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String jobId;
        private String forecastId;
        private boolean allowNoForecasts = true;

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            forecastId = in.readString();
            allowNoForecasts = in.readBoolean();
        }

        public Request(String jobId, String forecastId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.forecastId = ExceptionsHelper.requireNonNull(forecastId, ForecastRequestStats.FORECAST_ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public String getForecastId() {
            return forecastId;
        }

        public boolean isAllowNoForecasts() {
            return allowNoForecasts;
        }

        public void setAllowNoForecasts(boolean allowNoForecasts) {
            this.allowNoForecasts = allowNoForecasts;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(forecastId);
            out.writeBoolean(allowNoForecasts);
        }
    }

}
