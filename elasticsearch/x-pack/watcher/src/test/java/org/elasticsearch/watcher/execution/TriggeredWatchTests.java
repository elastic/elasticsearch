/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class TriggeredWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testParser() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", watcherHttpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(watch.id(), DateTime.now(DateTimeZone.UTC), DateTime.now(DateTimeZone.UTC));
        Wid wid = new Wid("_record", randomLong(), DateTime.now(DateTimeZone.UTC));
        TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        triggeredWatch.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        TriggeredWatch parsedTriggeredWatch = triggeredWatchParser().parse(triggeredWatch.id().value(), 0, jsonBuilder.bytes());

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedTriggeredWatch.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }

    private TriggeredWatch.Parser triggeredWatchParser() {
        return internalCluster().getInstance(TriggeredWatch.Parser.class);
    }
}
