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

package org.elasticsearch.client.ml.calendars;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

public class ScheduledEventTests extends AbstractXContentTestCase<ScheduledEvent> {

    public static ScheduledEvent testInstance(String calendarId, @Nullable String eventId) {
        Date start = new Date(randomNonNegativeLong());
        Date end = new Date(start.getTime() + randomIntBetween(1, 10000) * 1000);

        return new ScheduledEvent(randomAlphaOfLength(10), start, end, calendarId, eventId);
    }

    public static ScheduledEvent testInstance() {
        return testInstance(randomAlphaOfLengthBetween(1, 20),
            randomBoolean() ? null : randomAlphaOfLength(7));
    }

    @Override
    protected ScheduledEvent createTestInstance() {
        return testInstance();
    }

    @Override
    protected ScheduledEvent doParseInstance(XContentParser parser) {
        return ScheduledEvent.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
