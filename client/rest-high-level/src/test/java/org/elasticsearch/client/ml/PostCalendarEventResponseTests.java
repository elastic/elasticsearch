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

import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.client.ml.calendars.ScheduledEventTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostCalendarEventResponseTests extends AbstractXContentTestCase<PostCalendarEventResponse> {
    @Override
    protected PostCalendarEventResponse createTestInstance() {
        int numberOfEvents = randomIntBetween(1, 10);
        List<ScheduledEvent> events = new ArrayList<>(numberOfEvents);
        for (int i = 0; i < numberOfEvents; i++) {
            events.add(ScheduledEventTests.testInstance());
        }
        return new PostCalendarEventResponse(events);
    }

    @Override
    protected PostCalendarEventResponse doParseInstance(XContentParser parser) throws IOException {
        return PostCalendarEventResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
