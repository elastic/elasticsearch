/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.hamcrest.Matchers.equalTo;

public class TriggeredWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testParser() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", watcherHttpClient(), noopEmailService(),
                watcherSearchTemplateService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(watch.id(), DateTime.now(DateTimeZone.UTC), DateTime.now(DateTimeZone.UTC));
        Wid wid = new Wid("_record", DateTime.now(DateTimeZone.UTC));
        TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        triggeredWatch.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        TriggeredWatch parsedTriggeredWatch = triggeredWatchParser().parse(triggeredWatch.id().value(), 0, jsonBuilder.bytes());

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedTriggeredWatch.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().utf8ToString(), equalTo(jsonBuilder2.bytes().utf8ToString()));
    }

    private TriggeredWatch.Parser triggeredWatchParser() {
        return internalCluster().getInstance(TriggeredWatch.Parser.class);
    }

    protected WatcherSearchTemplateService watcherSearchTemplateService() {
        return internalCluster().getInstance(WatcherSearchTemplateService.class);
    }
}
