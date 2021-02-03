/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetCalendarsRequest implements Validatable, ToXContentObject {

    public static final ObjectParser<GetCalendarsRequest, Void> PARSER =
            new ObjectParser<>("get_calendars_request", GetCalendarsRequest::new);

    static {
        PARSER.declareString(GetCalendarsRequest::setCalendarId, Calendar.ID);
        PARSER.declareObject(GetCalendarsRequest::setPageParams, PageParams.PARSER, PageParams.PAGE);
    }

    private String calendarId;
    private PageParams pageParams;

    public GetCalendarsRequest() {
    }

    public GetCalendarsRequest(String calendarId) {
        this.calendarId = calendarId;
    }

    public String getCalendarId() {
        return calendarId;
    }

    public void setCalendarId(String calendarId) {
        this.calendarId = calendarId;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
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
        GetCalendarsRequest other = (GetCalendarsRequest) obj;
        return Objects.equals(calendarId, other.calendarId) && Objects.equals(pageParams, other.pageParams);
    }
}
