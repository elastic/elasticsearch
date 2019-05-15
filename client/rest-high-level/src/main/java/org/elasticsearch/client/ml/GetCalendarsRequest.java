/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetCalendarsRequest extends ActionRequest implements ToXContentObject {

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
    public ActionRequestValidationException validate() {
        return null;
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
