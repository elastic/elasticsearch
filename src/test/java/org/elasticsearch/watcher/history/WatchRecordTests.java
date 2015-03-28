/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.watcher.actions.email.EmailAction;
import org.elasticsearch.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.simple.AlwaysFalseCondition;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecution;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.junit.Test;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class WatchRecordTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testParser() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", scriptService(), httpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(DateTime.now(UTC), DateTime.now(UTC));
        WatchRecord watchRecord = new WatchRecord(watch, event);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        watchRecord.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        WatchRecord parsedWatchRecord = watchRecordParser().parse(jsonBuilder.bytes(), watchRecord.id(), 0);

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedWatchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }

    @Test
    public void testParser_WithSealedWatchRecord() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", scriptService(), httpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(DateTime.now(UTC), DateTime.now(UTC));
        WatchRecord watchRecord = new WatchRecord(watch, event);
        WatchExecutionContext ctx = new WatchExecutionContext(watchRecord.id(), watch, new DateTime(), event);
        ctx.onActionResult(new EmailAction.Result.Failure("failed to send because blah"));
        ctx.onActionResult(new WebhookAction.Result.Executed(300, "http://localhost:8000/watchfoo", "{'awesome' : 'us'}"));
        Input.Result inputResult = new SimpleInput.Result(SimpleInput.TYPE, new Payload.Simple());
        Condition.Result conditionResult = AlwaysTrueCondition.RESULT;
        ctx.onThrottleResult(Throttler.NO_THROTTLE.throttle(ctx));
        ctx.onInputResult(inputResult);
        ctx.onConditionResult(conditionResult);
        watchRecord.seal(new WatchExecution(ctx));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        watchRecord.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        WatchRecord parsedWatchRecord = watchRecordParser().parse(jsonBuilder.bytes(), watchRecord.id(), 0);

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedWatchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }

    @Test
    public void testParser_WithSealedWatchRecord_WithScriptSearchCondition() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", scriptService(), httpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(DateTime.now(UTC), DateTime.now(UTC));
        WatchRecord watchRecord = new WatchRecord(watch, event);
        WatchExecutionContext ctx = new WatchExecutionContext(watchRecord.id(), watch, new DateTime(), event);
        ctx.onActionResult(new EmailAction.Result.Failure("failed to send because blah"));
        ctx.onActionResult(new WebhookAction.Result.Executed(300, "http://localhost:8000/watchfoo", "{'awesome' : 'us'}"));
        Input.Result inputResult = new SimpleInput.Result(SimpleInput.TYPE, new Payload.Simple());
        Condition.Result conditionResult = AlwaysFalseCondition.RESULT;
        ctx.onThrottleResult(Throttler.NO_THROTTLE.throttle(ctx));
        ctx.onInputResult(inputResult);
        ctx.onConditionResult(conditionResult);
        watchRecord.seal(new WatchExecution(ctx));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        watchRecord.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        WatchRecord parsedWatchRecord = watchRecordParser().parse(jsonBuilder.bytes(), watchRecord.id(), 0);

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedWatchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }


}
