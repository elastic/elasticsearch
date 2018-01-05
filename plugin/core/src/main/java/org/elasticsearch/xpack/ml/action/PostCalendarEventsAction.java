/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class PostCalendarEventsAction extends Action<PostCalendarEventsAction.Request, PostCalendarEventsAction.Response,
        PostCalendarEventsAction.RequestBuilder> {
    public static final PostCalendarEventsAction INSTANCE = new PostCalendarEventsAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/events/post";

    public static final ParseField EVENTS = new ParseField("events");

    private PostCalendarEventsAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        public static Request parseRequest(String calendarId, BytesReference data, XContentType contentType) throws IOException {
            List<ScheduledEvent.Builder> events = new ArrayList<>();

            XContent xContent = contentType.xContent();
            int lineNumber = 0;
            int from = 0;
            int length = data.length();
            byte marker = xContent.streamSeparator();
            while (true) {
                int nextMarker = findNextMarker(marker, from, data, length);
                if (nextMarker == -1) {
                    break;
                }
                lineNumber++;

                try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, data.slice(from, nextMarker - from))) {
                    try {
                        ScheduledEvent.Builder event = ScheduledEvent.PARSER.apply(parser, null);
                        events.add(event);
                    } catch (ParsingException pe) {
                        throw ExceptionsHelper.badRequestException("Failed to parse scheduled event on line [" + lineNumber + "]", pe);
                    }

                    from = nextMarker + 1;
                }
            }

            for (ScheduledEvent.Builder event: events) {
                if (event.getCalendarId() != null && event.getCalendarId().equals(calendarId) == false) {
                    throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INCONSISTENT_ID,
                            Calendar.ID.getPreferredName(), event.getCalendarId(), calendarId));
                }

                // Set the calendar Id in case it is null
                event.calendarId(calendarId);
            }
            return new Request(calendarId, events.stream().map(ScheduledEvent.Builder::build).collect(Collectors.toList()));
        }

        private static int findNextMarker(byte marker, int from, BytesReference data, int length) {
            for (int i = from; i < length; i++) {
                if (data.get(i) == marker) {
                    return i;
                }
            }
            if (from != length) {
                throw new IllegalArgumentException("The post calendar events request must be terminated by a newline [\n]");
            }
            return -1;
        }

        private String calendarId;
        private List<ScheduledEvent> scheduledEvents;

        Request() {
        }

        public Request(String calendarId, List<ScheduledEvent> scheduledEvents) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
            this.scheduledEvents = ExceptionsHelper.requireNonNull(scheduledEvents, EVENTS.getPreferredName());
        }

        public String getCalendarId() {
            return calendarId;
        }

        public List<ScheduledEvent> getScheduledEvents() {
            return scheduledEvents;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calendarId = in.readString();
            scheduledEvents = in.readList(ScheduledEvent::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
            out.writeList(scheduledEvents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId, scheduledEvents);
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
            return Objects.equals(calendarId, other.calendarId) && Objects.equals(scheduledEvents, other.scheduledEvents);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        private List<ScheduledEvent> scheduledEvents;

        Response() {
        }

        public Response(List<ScheduledEvent> scheduledEvents) {
            super(true);
            this.scheduledEvents = scheduledEvents;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            in.readList(ScheduledEvent::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            out.writeList(scheduledEvents);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(EVENTS.getPreferredName(), scheduledEvents);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(isAcknowledged(), scheduledEvents);
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
            return Objects.equals(isAcknowledged(), other.isAcknowledged()) && Objects.equals(scheduledEvents, other.scheduledEvents);
        }
    }
}
