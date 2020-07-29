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
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteCalendarEventAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteCalendarEventAction INSTANCE = new DeleteCalendarEventAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/events/delete";

    private DeleteCalendarEventAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private String calendarId;
        private String eventId;

        public Request(StreamInput in) throws IOException {
            super(in);
            calendarId = in.readString();
            eventId = in.readString();
        }

        public Request(String calendarId, String eventId) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
            this.eventId = ExceptionsHelper.requireNonNull(eventId, ScheduledEvent.EVENT_ID.getPreferredName());
        }

        public String getCalendarId() {
            return calendarId;
        }

        public String getEventId() {
            return eventId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
            out.writeString(eventId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventId, calendarId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Request other = (Request) obj;
            return Objects.equals(eventId, other.eventId) && Objects.equals(calendarId, other.calendarId);
        }
    }
}
