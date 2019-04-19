/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.AbstractHlrcStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutCalendarActionResponseTests
    extends AbstractHlrcStreamableXContentTestCase<PutCalendarAction.Response, PutCalendarResponse> {

    @Override
    protected PutCalendarAction.Response createTestInstance() {
        return new PutCalendarAction.Response(testInstance());
    }

    @Override
    protected PutCalendarAction.Response doParseInstance(XContentParser parser) throws IOException {
        return new PutCalendarAction.Response(Calendar.LENIENT_PARSER.parse(parser, null).build());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    public PutCalendarResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return PutCalendarResponse.fromXContent(parser);
    }

    @Override
    public PutCalendarAction.Response convertHlrcToInternal(PutCalendarResponse instance) {
        org.elasticsearch.client.ml.calendars.Calendar hlrcCalendar = instance.getCalendar();
        Calendar internalCalendar = new Calendar(hlrcCalendar.getId(), hlrcCalendar.getJobIds(), hlrcCalendar.getDescription());
        return new PutCalendarAction.Response(internalCalendar);
    }

    @Override
    protected PutCalendarAction.Response createBlankInstance() {
        return new PutCalendarAction.Response();
    }

    public static Calendar testInstance() {
        return testInstance(new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray()).ofCodePointsLength(random(), 10, 10));
    }

    public static Calendar testInstance(String calendarId) {
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        String description = null;
        if (randomBoolean()) {
            description = randomAlphaOfLength(20);
        }
        return new Calendar(calendarId, items, description);
    }
}
