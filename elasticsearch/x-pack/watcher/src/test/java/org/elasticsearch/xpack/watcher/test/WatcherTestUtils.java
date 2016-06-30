/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.common.ScriptServiceProxy;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpMethod;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.secret.Secret;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.email.Authentication;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.EmailTemplate;
import org.elasticsearch.xpack.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.notification.email.Profile;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.actions.ExecutableActions;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.email.ExecutableEmailAction;
import org.elasticsearch.xpack.watcher.actions.webhook.ExecutableWebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.execution.Wid;
import org.elasticsearch.xpack.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.CronSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.mockito.Mockito;

import javax.mail.internet.AddressException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static java.util.Collections.emptyMap;
import static org.apache.lucene.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertThat;

/**
 *
 */
public final class WatcherTestUtils {

    public static final Payload EMPTY_PAYLOAD = new Payload.Simple(emptyMap());

    private WatcherTestUtils() {
    }

    public static XContentSource xContentSource(BytesReference bytes) {
        XContent xContent = XContentFactory.xContent(bytes);
        return new XContentSource(bytes, xContent.type());
    }

    public static void assertValue(Map<String, Object> map, String path, Matcher<?> matcher) {
        assertThat(ObjectPath.eval(path, map), (Matcher<Object>) matcher);
    }

    public static void assertValue(XContentSource source, String path, Matcher<?> matcher) {
        assertThat(source.getValue(path), (Matcher<Object>) matcher);
    }

    public static XContentParser xContentParser(XContentBuilder builder) throws IOException {
        return builder.contentType().xContent().createParser(builder.bytes());
    }

    public static SearchRequest newInputSearchRequest(String... indices) {
        SearchRequest request = new SearchRequest();
        request.indices(indices);
        request.indicesOptions(WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS);
        request.searchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE);
        return request;
    }

    public static SearchRequest matchAllRequest() {
        return matchAllRequest(null);
    }

    public static SearchRequest matchAllRequest(IndicesOptions indicesOptions) {
        SearchRequest request = new SearchRequest(Strings.EMPTY_ARRAY)
                .source(SearchSourceBuilder.searchSource().query(matchAllQuery()));
        if (indicesOptions != null) {
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    public static Payload simplePayload(String key, Object value) {
        return new Payload.Simple(key, value);
    }

    public static WatchExecutionContextMockBuilder mockExecutionContextBuilder(String watchId) {
        return new WatchExecutionContextMockBuilder(watchId)
                .wid(new Wid(watchId, randomInt(10), DateTime.now(UTC)));
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, randomInt(10), DateTime.now(UTC)))
                .payload(payload)
                .buildMock();
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, DateTime time, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, randomInt(10), DateTime.now(UTC)))
                .payload(payload)
                .time(watchId, time)
                .buildMock();
    }

    public static WatchExecutionContext mockExecutionContext(String watchId, DateTime executionTime, TriggerEvent event, Payload payload) {
        return mockExecutionContextBuilder(watchId)
                .wid(new Wid(watchId, randomInt(10), DateTime.now(UTC)))
                .payload(payload)
                .executionTime(executionTime)
                .triggerEvent(event)
                .buildMock();
    }


    public static Watch createTestWatch(String watchName, HttpClient httpClient, EmailService emailService,
                                        WatcherSearchTemplateService searchTemplateService, ESLogger logger) throws AddressException {
        WatcherClientProxy client = WatcherClientProxy.of(ESIntegTestCase.client());
        return createTestWatch(watchName, client, httpClient, emailService, searchTemplateService, logger);
    }


    public static Watch createTestWatch(String watchName, WatcherClientProxy client, HttpClient httpClient, EmailService emailService,
                                        WatcherSearchTemplateService searchTemplateService, ESLogger logger) throws AddressException {

        SearchRequest conditionRequest = newInputSearchRequest("my-condition-index").source(searchSource().query(matchAllQuery()));
        SearchRequest transformRequest = newInputSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        transformRequest.searchType(ExecutableSearchTransform.DEFAULT_SEARCH_TYPE);
        conditionRequest.searchType(ExecutableSearchInput.DEFAULT_SEARCH_TYPE);

        List<ActionWrapper> actions = new ArrayList<>();

        HttpRequestTemplate.Builder httpRequest = HttpRequestTemplate.builder("localhost", 80);
        httpRequest.method(HttpMethod.POST);

        TextTemplate path = TextTemplate.inline("/foobarbaz/{{ctx.watch_id}}").build();
        httpRequest.path(path);
        TextTemplate body = TextTemplate.inline("{{ctx.watch_id}} executed with {{ctx.payload.response.hits.total_hits}} hits").build();
        httpRequest.body(body);

        TextTemplateEngine engine = new MockTextTemplateEngine();

        actions.add(new ActionWrapper("_webhook", new ExecutableWebhookAction(new WebhookAction(httpRequest.build()), logger, httpClient,
                engine)));

        String from = "from@test.com";
        String to = "to@test.com";

        EmailTemplate email = EmailTemplate.builder()
                .from(from)
                .to(to)
                .build();

        Authentication auth = new Authentication("testname", new Secret("testpassword".toCharArray()));

        EmailAction action = new EmailAction(email, "testaccount", auth, Profile.STANDARD, null, null);
        ExecutableEmailAction executale = new ExecutableEmailAction(action, logger, emailService, engine,
                new HtmlSanitizer(Settings.EMPTY), Collections.emptyMap());

        actions.add(new ActionWrapper("_email", executale));

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("foo", "bar");

        Map<String, Object> inputData = new LinkedHashMap<>();
        inputData.put("bar", "foo");

        DateTime now = DateTime.now(UTC);
        Map<String, ActionStatus> statuses = new HashMap<>();
        statuses.put("_webhook", new ActionStatus(now));
        statuses.put("_email", new ActionStatus(now));

        SearchTransform searchTransform = new SearchTransform(new WatcherSearchTemplateRequest(transformRequest), null, null);

        return new Watch(
                watchName,
                new ScheduleTrigger(new CronSchedule("0/5 * * * * ? *")),
                new ExecutableSimpleInput(new SimpleInput(new Payload.Simple(inputData)), logger),
                new ExecutableAlwaysCondition(logger),
                new ExecutableSearchTransform(searchTransform, logger, client, searchTemplateService, null),
                new TimeValue(0),
                new ExecutableActions(actions),
                metadata,
                new WatchStatus(now, statuses));
    }

    public static ScriptServiceProxy getScriptServiceProxy(ThreadPool tp) throws Exception {
        Settings settings = Settings.builder()
                .put("script.inline", "true")
                .put("script.indexed", "true")
                .put("path.home", createTempDir())
                .build();
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.singletonList(ScriptServiceProxy.INSTANCE));

        ScriptEngineRegistry scriptEngineRegistry =
                new ScriptEngineRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        return  ScriptServiceProxy.of(new ScriptService(settings, new Environment(settings),
                new ResourceWatcherService(settings, tp), scriptEngineRegistry, scriptContextRegistry, scriptSettings));
    }

    public static SearchType getRandomSupportedSearchType() {
        return randomFrom(
                SearchType.QUERY_AND_FETCH,
                SearchType.QUERY_THEN_FETCH,
                SearchType.DFS_QUERY_THEN_FETCH,
                SearchType.DFS_QUERY_AND_FETCH);
    }
}
