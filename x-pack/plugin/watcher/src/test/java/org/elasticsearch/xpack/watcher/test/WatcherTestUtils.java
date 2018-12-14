/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.email.ExecutableEmailAction;
import org.elasticsearch.xpack.watcher.actions.webhook.ExecutableWebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.notification.email.Authentication;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.EmailTemplate;
import org.elasticsearch.xpack.watcher.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.watcher.notification.email.Profile;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.trigger.schedule.CronSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import javax.mail.internet.AddressException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.joda.time.DateTimeZone.UTC;

public final class WatcherTestUtils {

    private WatcherTestUtils() {
    }

    public static XContentSource xContentSource(BytesReference bytes) {
        XContent xContent = XContentFactory.xContent(XContentHelper.xContentType(bytes));
        return new XContentSource(bytes, xContent.type());
    }

    public static WatcherSearchTemplateRequest templateRequest(SearchSourceBuilder sourceBuilder, String... indices) {
        return templateRequest(sourceBuilder, SearchType.DEFAULT, indices);
    }

    public static WatcherSearchTemplateRequest templateRequest(SearchSourceBuilder sourceBuilder, SearchType searchType,
                                                               String... indices) {
        try {
            XContentBuilder xContentBuilder = jsonBuilder();
            xContentBuilder.value(sourceBuilder);
            return new WatcherSearchTemplateRequest(indices, new String[0], searchType,
                    WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS, BytesReference.bytes(xContentBuilder));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static WatchExecutionContextMockBuilder mockExecutionContextBuilder(String watchId) {
        return new WatchExecutionContextMockBuilder(watchId)
                .wid(new Wid(watchId, DateTime.now(UTC)));
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, DateTime.now(UTC)))
                .payload(payload)
                .buildMock();
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, DateTime time, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, DateTime.now(UTC)))
                .payload(payload)
                .time(watchId, time)
                .buildMock();
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, DateTime executionTime, TriggerEvent event, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, DateTime.now(UTC)))
                .payload(payload)
                .executionTime(executionTime)
                .triggerEvent(event)
                .buildMock();
    }

    public static WatchExecutionContext createWatchExecutionContext() throws Exception {
        Watch watch = new Watch("test-watch",
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                new ExecutableSimpleInput(new SimpleInput(new Payload.Simple())),
                InternalAlwaysCondition.INSTANCE,
                null,
                null,
                new ArrayList<>(),
                null,
                new WatchStatus(new DateTime(0, UTC), emptyMap()), 1L);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent(watch.id(), new DateTime(0, UTC), new DateTime(0, UTC)),
                TimeValue.timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);
        return context;
    }


    public static Watch createTestWatch(String watchName, Client client, HttpClient httpClient, EmailService emailService,
                                        WatcherSearchTemplateService searchTemplateService, Logger logger) throws AddressException {
        List<ActionWrapper> actions = new ArrayList<>();
        TextTemplateEngine engine = new MockTextTemplateEngine();

        HttpRequestTemplate.Builder httpRequest = HttpRequestTemplate.builder("localhost", 80);
        httpRequest.method(HttpMethod.POST);
        httpRequest.path(new TextTemplate("/foobarbaz/{{ctx.watch_id}}"));
        httpRequest.body(new TextTemplate("{{ctx.watch_id}} executed with {{ctx.payload.response.hits.total_hits}} hits"));
        actions.add(new ActionWrapper("_webhook", null, null, null, new ExecutableWebhookAction(new WebhookAction(httpRequest.build()),
                logger, httpClient, engine)));


        EmailTemplate email = EmailTemplate.builder().from("from@test.com").to("to@test.com").build();
        Authentication auth = new Authentication("testname", new Secret("testpassword".toCharArray()));
        EmailAction action = new EmailAction(email, "testaccount", auth, Profile.STANDARD, null, null);
        ExecutableEmailAction executale = new ExecutableEmailAction(action, logger, emailService, engine,
                new HtmlSanitizer(Settings.EMPTY), Collections.emptyMap());
        actions.add(new ActionWrapper("_email", null, null, null, executale));

        DateTime now = DateTime.now(UTC);
        Map<String, ActionStatus> statuses = new HashMap<>();
        statuses.put("_webhook", new ActionStatus(now));
        statuses.put("_email", new ActionStatus(now));

        WatcherSearchTemplateRequest transformRequest = templateRequest(searchSource().query(matchAllQuery()), "my-payload-index");
        SearchTransform searchTransform = new SearchTransform(transformRequest, null, null);

        return new Watch(
                watchName,
                new ScheduleTrigger(new CronSchedule("0/5 * * * * ? *")),
                new ExecutableSimpleInput(new SimpleInput(new Payload.Simple(Collections.singletonMap("bar", "foo")))),
                InternalAlwaysCondition.INSTANCE,
                new ExecutableSearchTransform(searchTransform, logger, client, searchTemplateService, TimeValue.timeValueMinutes(1)),
                new TimeValue(0),
                actions,
                Collections.singletonMap("foo", "bar"),
                new WatchStatus(now, statuses), 1L);
    }

    public static SearchType getRandomSupportedSearchType() {
        return randomFrom(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
    }
}
