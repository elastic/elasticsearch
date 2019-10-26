/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;


import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.hamcrest.Matchers.is;

public class ScheduleTriggerEventTests extends ESTestCase {
    public void testParserRandomDateMath() throws Exception {
        String triggeredTime = randomFrom("now", "now+5m", "2015-05-07T22:24:41.254Z", "2015-05-07T22:24:41.254Z||-5m");
        String scheduledTime = randomFrom("now", "now-5m", "2015-05-07T22:24:41.254Z", "2015-05-07T22:24:41.254Z||+5h");
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(ScheduleTriggerEvent.Field.SCHEDULED_TIME.getPreferredName(), scheduledTime);
        jsonBuilder.field(ScheduleTriggerEvent.Field.TRIGGERED_TIME.getPreferredName(), triggeredTime);
        jsonBuilder.endObject();

        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();

        ScheduleTriggerEvent scheduleTriggerEvent = ScheduleTriggerEvent.parse(parser, "_id", "_context", Clock.systemUTC());
        assertThat(scheduleTriggerEvent.scheduledTime().isAfter(Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC)), is(true));
        assertThat(scheduleTriggerEvent.triggeredTime().isAfter(Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC)), is(true));
    }
}
