/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.calendars.SpecialEvent;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class PostCalendarEventsAction extends Action<PostCalendarEventsAction.Request, PostCalendarEventsAction.Response,
        PostCalendarEventsAction.RequestBuilder> {
    public static final PostCalendarEventsAction INSTANCE = new PostCalendarEventsAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/events/post";

    public static final ParseField SPECIAL_EVENTS = new ParseField("special_events");

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
            List<SpecialEvent.Builder> events = new ArrayList<>();

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
                        SpecialEvent.Builder event = SpecialEvent.PARSER.apply(parser, null);
                        events.add(event);
                    } catch (ParsingException pe) {
                        throw ExceptionsHelper.badRequestException("Failed to parse special event on line [" + lineNumber + "]", pe);
                    }

                    from = nextMarker + 1;
                }
            }

            for (SpecialEvent.Builder event: events) {
                if (event.getCalendarId() != null && event.getCalendarId().equals(calendarId) == false) {
                    throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INCONSISTENT_ID,
                            Calendar.ID.getPreferredName(), event.getCalendarId(), calendarId));
                }

                // Set the calendar Id in case it is null
                event.calendarId(calendarId);
            }
            return new Request(calendarId, events.stream().map(SpecialEvent.Builder::build).collect(Collectors.toList()));
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
        private List<SpecialEvent> specialEvents;

        Request() {
        }

        public Request(String calendarId, List<SpecialEvent> specialEvents) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
            this.specialEvents = ExceptionsHelper.requireNonNull(specialEvents, SPECIAL_EVENTS.getPreferredName());
        }

        public String getCalendarId() {
            return calendarId;
        }

        public List<SpecialEvent> getSpecialEvents() {
            return specialEvents;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calendarId = in.readString();
            specialEvents = in.readList(SpecialEvent::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
            out.writeList(specialEvents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId, specialEvents);
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
            return Objects.equals(calendarId, other.calendarId) && Objects.equals(specialEvents, other.specialEvents);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        private List<SpecialEvent> specialEvent;

        Response() {
        }

        public Response(List<SpecialEvent> specialEvents) {
            super(true);
            this.specialEvent = specialEvents;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            in.readList(SpecialEvent::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            out.writeList(specialEvent);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SPECIAL_EVENTS.getPreferredName(), specialEvent);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(isAcknowledged(), specialEvent);
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
            return Objects.equals(isAcknowledged(), other.isAcknowledged()) && Objects.equals(specialEvent, other.specialEvent);
        }
    }
}
