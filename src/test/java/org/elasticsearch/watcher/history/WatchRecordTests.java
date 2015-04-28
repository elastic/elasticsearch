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
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.email.EmailAction;
import org.elasticsearch.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecution;
import org.junit.Test;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class WatchRecordTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testParser() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", scriptService(), watcherHttpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(watch.id(), DateTime.now(UTC), DateTime.now(UTC));
        Wid wid = new Wid("_record", randomLong(), DateTime.now(UTC));
        WatchRecord watchRecord = new WatchRecord(wid, watch, event);
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        watchRecord.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        WatchRecord parsedWatchRecord = watchRecordParser().parse(watchRecord.id().value(), 0, jsonBuilder.bytes());

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedWatchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }

    @Test
    public void testParser_WithSealedWatchRecord() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", scriptService(), watcherHttpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(watch.id(), DateTime.now(UTC), DateTime.now(UTC));
        Wid wid = new Wid("_record", randomLong(), DateTime.now(UTC));
        WatchRecord watchRecord = new WatchRecord(wid, watch, event);
        WatchExecutionContext ctx = new TriggeredExecutionContext(watch, new DateTime(), event);
        ctx.onActionResult(new ActionWrapper.Result("_email", new EmailAction.Result.Failure("failed to send because blah")));
        HttpRequest request = HttpRequest.builder("localhost", 8000)
                .path("/watchfoo")
                .body("{'awesome' : 'us'}")
                .build();
        ctx.onActionResult(new ActionWrapper.Result("_webhook", new WebhookAction.Result.Executed(request, new HttpResponse(300))));
        SimpleInput.Result inputResult = new SimpleInput.Result(new Payload.Simple());
        Condition.Result conditionResult = AlwaysCondition.Result.INSTANCE;
        ctx.onThrottleResult(Throttler.NO_THROTTLE.throttle(ctx));
        ctx.onInputResult(inputResult);
        ctx.onConditionResult(conditionResult);
        watchRecord.seal(new WatchExecution(ctx));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        watchRecord.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        WatchRecord parsedWatchRecord = watchRecordParser().parse(watchRecord.id().value(), 0, jsonBuilder.bytes());

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedWatchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }

    @Test
    public void testParser_WithSealedWatchRecord_WithScriptSearchCondition() throws Exception {
        Watch watch = WatcherTestUtils.createTestWatch("fired_test", scriptService(), watcherHttpClient(), noopEmailService(), logger);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(watch.id(), DateTime.now(UTC), DateTime.now(UTC));
        WatchExecutionContext ctx = new TriggeredExecutionContext( watch, new DateTime(), event);
        WatchRecord watchRecord = new WatchRecord(ctx.id(), watch, event);
        ctx.onActionResult(new ActionWrapper.Result("_email", new EmailAction.Result.Failure("failed to send because blah")));
        HttpRequest request = HttpRequest.builder("localhost", 8000)
                .path("/watchfoo")
                .body("{'awesome' : 'us'}")
                .build();
        ctx.onActionResult(new ActionWrapper.Result("_webhook", new WebhookAction.Result.Executed(request, new HttpResponse(300))));
        SimpleInput.Result inputResult = new SimpleInput.Result(new Payload.Simple());
        Condition.Result conditionResult = AlwaysCondition.Result.INSTANCE;
        ctx.onThrottleResult(Throttler.NO_THROTTLE.throttle(ctx));
        ctx.onInputResult(inputResult);
        ctx.onConditionResult(conditionResult);
        watchRecord.seal(new WatchExecution(ctx));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        watchRecord.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        WatchRecord parsedWatchRecord = watchRecordParser().parse(watchRecord.id().value(), 0, jsonBuilder.bytes());

        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedWatchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertThat(jsonBuilder.bytes().toUtf8(), equalTo(jsonBuilder2.bytes().toUtf8()));
    }


}
