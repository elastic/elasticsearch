/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutCalendarAction extends ActionType<PutCalendarAction.Response> {
    public static final PutCalendarAction INSTANCE = new PutCalendarAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/put";

    private PutCalendarAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements ToXContentObject {

        public static Request parseRequest(String calendarId, XContentParser parser) {
            Calendar.Builder builder = Calendar.STRICT_PARSER.apply(parser, null);
            if (builder.getId() == null) {
                builder.setId(calendarId);
            } else if (Strings.isNullOrEmpty(calendarId) == false && calendarId.equals(builder.getId()) == false) {
                // If we have both URI and body filter ID, they must be identical
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, Calendar.ID.getPreferredName(), builder.getId(), calendarId)
                );
            }
            return new Request(builder.build());
        }

        private final Calendar calendar;

        public Request(StreamInput in) throws IOException {
            super(in);
            calendar = new Calendar(in);
        }

        public Request(Calendar calendar) {
            this.calendar = ExceptionsHelper.requireNonNull(calendar, "calendar");
        }

        public Calendar getCalendar() {
            return calendar;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if ("_all".equals(calendar.getId())) {
                validationException = addValidationError("Cannot create a Calendar with the reserved name [_all]", validationException);
            }
            if (MlStrings.isValidId(calendar.getId()) == false) {
                validationException = addValidationError(
                    Messages.getMessage(Messages.INVALID_ID, Calendar.ID.getPreferredName(), calendar.getId()),
                    validationException
                );
            }
            if (MlStrings.hasValidLengthForId(calendar.getId()) == false) {
                validationException = addValidationError(
                    Messages.getMessage(Messages.JOB_CONFIG_ID_TOO_LONG, MlStrings.ID_LENGTH_LIMIT),
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            calendar.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            calendar.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendar);
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
            return Objects.equals(calendar, other.calendar);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Calendar calendar;

        public Response(Calendar calendar) {
            this.calendar = calendar;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            calendar.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return calendar.toXContent(builder, params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendar);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(calendar, other.calendar);
        }
    }
}
