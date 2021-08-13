/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
