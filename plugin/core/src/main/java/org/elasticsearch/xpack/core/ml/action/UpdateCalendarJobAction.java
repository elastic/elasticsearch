/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class UpdateCalendarJobAction extends Action<UpdateCalendarJobAction.Request, PutCalendarAction.Response,
        UpdateCalendarJobAction.RequestBuilder> {
    public static final UpdateCalendarJobAction INSTANCE = new UpdateCalendarJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/jobs/update";

    private UpdateCalendarJobAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public PutCalendarAction.Response newResponse() {
        return new PutCalendarAction.Response();
    }

    public static class Request extends ActionRequest {

        private String calendarId;
        private String jobIdToAdd;
        private String jobIdToRemove;

        public Request() {
        }

        public Request(String calendarId, String jobIdToAdd, String jobIdToRemove) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
            this.jobIdToAdd = jobIdToAdd;
            this.jobIdToRemove = jobIdToRemove;
        }

        public String getCalendarId() {
            return calendarId;
        }

        public String getJobIdToAdd() {
            return jobIdToAdd;
        }

        public String getJobIdToRemove() {
            return jobIdToRemove;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calendarId = in.readString();
            jobIdToAdd = in.readOptionalString();
            jobIdToRemove = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
            out.writeOptionalString(jobIdToAdd);
            out.writeOptionalString(jobIdToRemove);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId, jobIdToAdd, jobIdToRemove);
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
            return Objects.equals(calendarId, other.calendarId) && Objects.equals(jobIdToAdd, other.jobIdToAdd)
                    && Objects.equals(jobIdToRemove, other.jobIdToRemove);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, PutCalendarAction.Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }
}

