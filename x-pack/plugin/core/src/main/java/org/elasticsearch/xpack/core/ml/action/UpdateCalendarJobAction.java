/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class UpdateCalendarJobAction extends ActionType<PutCalendarAction.Response> {
    public static final UpdateCalendarJobAction INSTANCE = new UpdateCalendarJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/jobs/update";

    private UpdateCalendarJobAction() {
        super(NAME, PutCalendarAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private String calendarId;
        private String jobIdsToAddExpression;
        private String jobIdsToRemoveExpression;

        public Request(StreamInput in) throws IOException {
            super(in);
            calendarId = in.readString();
            jobIdsToAddExpression = in.readOptionalString();
            jobIdsToRemoveExpression = in.readOptionalString();
        }

        /**
         * Job id expressions may be a single job, job group or comma separated
         * list of job Ids or groups
         */
        public Request(String calendarId, String jobIdsToAddExpression, String jobIdsToRemoveExpression) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
            this.jobIdsToAddExpression = jobIdsToAddExpression;
            this.jobIdsToRemoveExpression = jobIdsToRemoveExpression;
        }

        public String getCalendarId() {
            return calendarId;
        }

        public String getJobIdsToAddExpression() {
            return jobIdsToAddExpression;
        }

        public String getJobIdsToRemoveExpression() {
            return jobIdsToRemoveExpression;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
            out.writeOptionalString(jobIdsToAddExpression);
            out.writeOptionalString(jobIdsToRemoveExpression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId, jobIdsToAddExpression, jobIdsToRemoveExpression);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(calendarId, other.calendarId)
                && Objects.equals(jobIdsToAddExpression, other.jobIdsToAddExpression)
                && Objects.equals(jobIdsToRemoveExpression, other.jobIdsToRemoveExpression);
        }
    }
}
