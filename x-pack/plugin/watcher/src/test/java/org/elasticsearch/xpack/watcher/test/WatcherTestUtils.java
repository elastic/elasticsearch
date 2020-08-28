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
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
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
import org.hamcrest.Matcher;

import javax.mail.internet.AddressException;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
import static org.hamcrest.Matchers.is;

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
            return new WatcherSearchTemplateRequest(indices, searchType,
                    WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS, BytesReference.bytes(xContentBuilder));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static WatchExecutionContextMockBuilder mockExecutionContextBuilder(String watchId) {
        return new WatchExecutionContextMockBuilder(watchId)
                .wid(new Wid(watchId, ZonedDateTime.now(ZoneOffset.UTC)));
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, ZonedDateTime.now(ZoneOffset.UTC)))
                .payload(payload)
                .buildMock();
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, ZonedDateTime time, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, ZonedDateTime.now(ZoneOffset.UTC)))
                .payload(payload)
                .time(watchId, time)
                .buildMock();
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, ZonedDateTime executionTime, TriggerEvent event,
                                                             Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, ZonedDateTime.now(ZoneOffset.UTC)))
                .payload(payload)
                .executionTime(executionTime)
                .triggerEvent(event)
                .buildMock();
    }

    public static WatchExecutionContext createWatchExecutionContext() throws Exception {
        ZonedDateTime EPOCH_UTC = Instant.EPOCH.atZone(ZoneOffset.UTC);
        Watch watch = new Watch("test-watch",
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                new ExecutableSimpleInput(new SimpleInput(new Payload.Simple())),
                InternalAlwaysCondition.INSTANCE,
                null,
                null,
                new ArrayList<>(),
                null,

                new WatchStatus(EPOCH_UTC, emptyMap()), 1L, 1L);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), EPOCH_UTC,
            new ScheduleTriggerEvent(watch.id(), EPOCH_UTC, EPOCH_UTC), TimeValue.timeValueSeconds(5));
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
                logger, httpClient, engine), null, null));


        EmailTemplate email = EmailTemplate.builder().from("from@test.com").to("to@test.com").build();
        Authentication auth = new Authentication("testname", new Secret("testpassword".toCharArray()));
        EmailAction action = new EmailAction(email, "testaccount", auth, Profile.STANDARD, null, null);
        ExecutableEmailAction executale = new ExecutableEmailAction(action, logger, emailService, engine,
                new HtmlSanitizer(Settings.EMPTY), Collections.emptyMap());
        actions.add(new ActionWrapper("_email", null, null, null, executale, null, null));

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
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
                new WatchStatus(now, statuses), 1L, 1L);
    }

    public static SearchType getRandomSupportedSearchType() {
        return randomFrom(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
    }

    public static Matcher<String> isSameDate(ZonedDateTime zonedDateTime) {
        /*
        When comparing timestamps returned from _search/.watcher-history* the same format of date has to be used
        during serialisation to json on index time.
        The toString of ZonedDateTime is omitting the millisecond part when is 0. This was not the case in joda.
         */
        return is(WatcherDateTimeUtils.formatDate(zonedDateTime));
    }
}
