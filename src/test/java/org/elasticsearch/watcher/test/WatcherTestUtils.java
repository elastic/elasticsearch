/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Actions;
import org.elasticsearch.watcher.actions.email.EmailAction;
import org.elasticsearch.watcher.actions.email.service.Authentication;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.Profile;
import org.elasticsearch.watcher.actions.webhook.HttpClient;
import org.elasticsearch.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.input.search.SearchInput;
import org.elasticsearch.watcher.scheduler.schedule.CronSchedule;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.template.ScriptTemplate;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.transform.SearchTransform;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import javax.mail.internet.AddressException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public final class WatcherTestUtils {

    public static final Payload EMPTY_PAYLOAD = new Payload.Simple(ImmutableMap.<String, Object>of());

    private WatcherTestUtils() {
    }

    public static SearchRequest newInputSearchRequest(String... indices) {
        SearchRequest request = new SearchRequest(indices);
        request.indicesOptions(WatcherUtils.DEFAULT_INDICES_OPTIONS);
        request.searchType(SearchInput.DEFAULT_SEARCH_TYPE);
        return request;
    }

    public static SearchRequest matchAllRequest() {
        return matchAllRequest(null);
    }

    public static SearchRequest matchAllRequest(IndicesOptions indicesOptions) {
        SearchRequest request = new SearchRequest(Strings.EMPTY_ARRAY)
                .source(SearchSourceBuilder.searchSource().query(matchAllQuery()).buildAsBytes(XContentType.JSON), false);
        if (indicesOptions != null) {
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    public static Payload simplePayload(String key, Object value) {
        return new Payload.Simple(key, value);
    }

    public static WatchExecutionContext mockExecutionContext(String watchName, Payload payload) {
        return mockExecutionContext(DateTime.now(), watchName, payload);
    }

    public static WatchExecutionContext mockExecutionContext(DateTime time, String watchName, Payload payload) {
        return mockExecutionContext(time, time, time, watchName, payload);
    }

    public static WatchExecutionContext mockExecutionContext(DateTime executionTime, DateTime firedTime, DateTime scheduledTime, String watchName, Payload payload) {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(ctx.executionTime()).thenReturn(executionTime);
        when(ctx.fireTime()).thenReturn(firedTime);
        when(ctx.scheduledTime()).thenReturn(scheduledTime);
        Watch watch = mock(Watch.class);
        when(watch.name()).thenReturn(watchName);
        when(ctx.watch()).thenReturn(watch);
        when(ctx.payload()).thenReturn(payload);
        return ctx;
    }

    public static Watch createTestWatch(String watchName, ScriptServiceProxy scriptService, HttpClient httpClient, EmailService emailService, ESLogger logger) throws AddressException {
        SearchRequest conditionRequest = newInputSearchRequest("my-condition-index").source(searchSource().query(matchAllQuery()));
        SearchRequest transformRequest = newInputSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        transformRequest.searchType(SearchTransform.DEFAULT_SEARCH_TYPE);
        conditionRequest.searchType(SearchInput.DEFAULT_SEARCH_TYPE);

        List<Action> actions = new ArrayList<>();

        Template url = new ScriptTemplate(scriptService, "http://localhost/foobarbaz/{{watch_name}}");
        Template body = new ScriptTemplate(scriptService, "{{watch_name}} executed with {{response.hits.total}} hits");

        actions.add(new WebhookAction(logger, null, httpClient, HttpMethod.GET, url, body));

        Email.Address from = new Email.Address("from@test.com");
        List<Email.Address> emailAddressList = new ArrayList<>();
        emailAddressList.add(new Email.Address("to@test.com"));
        Email.AddressList to = new Email.AddressList(emailAddressList);


        Email.Builder emailBuilder = Email.builder().id("prototype");
        emailBuilder.from(from);
        emailBuilder.to(to);


        EmailAction emailAction = new EmailAction(logger, null, emailService, emailBuilder.build(),
                new Authentication("testname", "testpassword"), Profile.STANDARD, "testaccount", body, body, null, true);

        actions.add(emailAction);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("foo", "bar");

        return new Watch(
                watchName,
                SystemClock.INSTANCE,
                new CronSchedule("0/5 * * * * ? *"),
                new SearchInput(logger, scriptService, ClientProxy.of(ElasticsearchIntegrationTest.client()), conditionRequest),
                new ScriptCondition(logger, scriptService, new Script("return true")),
                new SearchTransform(logger, scriptService, ClientProxy.of(ElasticsearchIntegrationTest.client()), transformRequest),
                new Actions(actions),
                metadata,
                new TimeValue(0),
                new Watch.Status());
    }
}
