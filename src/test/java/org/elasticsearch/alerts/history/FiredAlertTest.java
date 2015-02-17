/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.alerts.*;
import org.elasticsearch.alerts.actions.email.EmailAction;
import org.elasticsearch.alerts.actions.webhook.WebhookAction;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.search.ScriptSearchCondition;
import org.elasticsearch.alerts.condition.search.SearchCondition;
import org.elasticsearch.alerts.condition.simple.SimpleCondition;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

/**
 */
public class FiredAlertTest extends AbstractAlertingTests {

    @Test
    public void testParser() throws Exception {

        Alert alert = createTestAlert("fired_test");
        FiredAlert firedAlert = new FiredAlert(alert, new DateTime(), new DateTime());
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        firedAlert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        FiredAlert parsedFiredAlert = firedAlertParser().parse(jsonBuilder.bytes(), firedAlert.id(), 0);


        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedFiredAlert.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

        assertEquals(jsonBuilder.bytes().toUtf8(), jsonBuilder2.bytes().toUtf8());

    }

    @Test
    public void testParser_WithSealedFiredAlert() throws Exception {
        Alert alert = createTestAlert("fired_test");
        FiredAlert firedAlert = new FiredAlert(alert, new DateTime(), new DateTime());
        ExecutionContext ctx = new ExecutionContext(firedAlert.id(), alert, new DateTime(), new DateTime());
        ctx.onActionResult(new EmailAction.Result.Failure("failed to send because blah"));
        ctx.onActionResult(new WebhookAction.Result.Executed(300, "http://localhost:8000/alertfoo", "{'awesome' : 'us'}"));
        Condition.Result conditionResult = new SimpleCondition.Result(new Payload.Simple());
        ctx.onThrottleResult(Throttler.NO_THROTTLE.throttle(ctx, conditionResult));
        ctx.onConditionResult(conditionResult);
        firedAlert.update(new AlertExecution(ctx));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        firedAlert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        FiredAlert parsedFiredAlert = firedAlertParser().parse(jsonBuilder.bytes(), firedAlert.id(), 0);
        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedFiredAlert.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);
        assertEquals(jsonBuilder.bytes().toUtf8(), jsonBuilder2.bytes().toUtf8());
    }

    @Test
    public void testParser_WithSealedFiredAlert_WithScriptSearchCondition() throws Exception {
        Alert alert = createTestAlert("fired_test");
        FiredAlert firedAlert = new FiredAlert(alert, new DateTime(), new DateTime());
        ExecutionContext ctx = new ExecutionContext(firedAlert.id(), alert, new DateTime(), new DateTime());
        ctx.onActionResult(new EmailAction.Result.Failure("failed to send because blah"));
        ctx.onActionResult(new WebhookAction.Result.Executed(300, "http://localhost:8000/alertfoo", "{'awesome' : 'us'}"));
        Condition.Result conditionResult = new SearchCondition.Result(ScriptSearchCondition.TYPE, true, createConditionSearchRequest(), new Payload.Simple());
        ctx.onThrottleResult(Throttler.NO_THROTTLE.throttle(ctx, conditionResult));
        ctx.onConditionResult(conditionResult);
        firedAlert.update(new AlertExecution(ctx));

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        firedAlert.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        FiredAlert parsedFiredAlert = firedAlertParser().parse(jsonBuilder.bytes(), firedAlert.id(), 0);
        XContentBuilder jsonBuilder2 = XContentFactory.jsonBuilder();
        parsedFiredAlert.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);
        assertEquals(jsonBuilder.bytes().toUtf8(), jsonBuilder2.bytes().toUtf8());
    }


}
