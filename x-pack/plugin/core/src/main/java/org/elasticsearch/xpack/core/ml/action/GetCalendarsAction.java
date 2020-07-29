/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetCalendarsAction extends ActionType<GetCalendarsAction.Response> {

    public static final GetCalendarsAction INSTANCE = new GetCalendarsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/calendars/get";

    private GetCalendarsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final String ALL = "_all";

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setCalendarId, Calendar.ID);
            PARSER.declareObject(Request::setPageParams, PageParams.PARSER, PageParams.PAGE);
        }

        public static Request parseRequest(String calendarId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (calendarId != null) {
                request.setCalendarId(calendarId);
            }
            return request;
        }

        private String calendarId;
        private PageParams pageParams;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            calendarId = in.readOptionalString();
            pageParams = in.readOptionalWriteable(PageParams::new);
        }

        public void setCalendarId(String calendarId) {
            this.calendarId = calendarId;
        }

        public String getCalendarId() {
            return calendarId;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (calendarId != null && pageParams != null) {
                validationException = addValidationError("Params [" + PageParams.FROM.getPreferredName()
                                + ", " + PageParams.SIZE.getPreferredName() + "] are incompatible with ["
                                + Calendar.ID.getPreferredName() + "].",
                        validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(calendarId);
            out.writeOptionalWriteable(pageParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId, pageParams);
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
            return Objects.equals(calendarId, other.calendarId) && Objects.equals(pageParams, other.pageParams);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (calendarId != null) {
                builder.field(Calendar.ID.getPreferredName(), calendarId);
            }
            if (pageParams != null) {
                builder.field(PageParams.PAGE.getPreferredName(), pageParams);
            }
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends AbstractGetResourcesResponse<Calendar> implements StatusToXContentObject {

        public Response(QueryPage<Calendar> calendars) {
            super(calendars);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        public QueryPage<Calendar> getCalendars() {
            return getResources();
        }

        protected Reader<Calendar> getReader() {
            return Calendar::new;
        }
    }
}
