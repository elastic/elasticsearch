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
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PutCalendarActionResponseTests extends AbstractResponseTestCase<PutCalendarAction.Response, PutCalendarResponse> {

    @Override
    protected PutCalendarAction.Response createServerTestInstance(XContentType xContentType) {
        String calendarId = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray()).ofCodePointsLength(random(), 10, 10);
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        String description = null;
        if (randomBoolean()) {
            description = randomAlphaOfLength(20);
        }
        Calendar calendar = new Calendar(calendarId, items, description);
        return new PutCalendarAction.Response(calendar);
    }

    @Override
    protected PutCalendarResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return PutCalendarResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(PutCalendarAction.Response serverTestInstance, PutCalendarResponse clientInstance) {
        org.elasticsearch.client.ml.calendars.Calendar hlrcCalendar = clientInstance.getCalendar();
        Calendar internalCalendar = new Calendar(hlrcCalendar.getId(), hlrcCalendar.getJobIds(), hlrcCalendar.getDescription());
        PutCalendarAction.Response convertedServerTestInstance =new PutCalendarAction.Response(internalCalendar);
        assertThat(convertedServerTestInstance, equalTo(serverTestInstance));
    }
}
