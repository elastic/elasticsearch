/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.actions.TransformMocks;
import org.elasticsearch.watcher.actions.email.service.Authentication;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.Profile;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpMethod;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.template.ScriptTemplate;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.mail.internet.AddressException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class WebhookActionTests extends ElasticsearchTestCase {

    static final String TEST_BODY = "ERROR HAPPENED";
    static final String TEST_URL = "http://test.com/testurl";

    private ThreadPool tp = null;
    private ScriptServiceProxy scriptService;

    @Before
    public void init() throws IOException {
        tp = new ThreadPool(ThreadPool.Names.SAME);
        Settings settings = ImmutableSettings.settingsBuilder().build();
        MustacheScriptEngineService mustacheScriptEngineService = new MustacheScriptEngineService(settings);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(mustacheScriptEngineService);
        scriptService = ScriptServiceProxy.of(new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp), new NodeSettingsService(settings)));
    }

    @After
    public void cleanup() {
        tp.shutdownNow();
    }

    @Test @Repeat(iterations = 30)
    public void testExecute() throws Exception {
        ClientProxy client = mock(ClientProxy.class);
        ExecuteScenario scenario = randomFrom(ExecuteScenario.values());

        HttpClient httpClient = scenario.client();
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT);

        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        final String account = "account1";

        WebhookAction webhookAction =
                new WebhookAction(logger,
                        transform,
                        httpClient,
                        method,
                        new ScriptTemplate(scriptService, TEST_URL),
                        new ScriptTemplate(scriptService, TEST_BODY));

        Watch watch = createWatch("test_watch", client, account);
        WatchExecutionContext ctx = new WatchExecutionContext("testid", watch, new DateTime(), new ScheduleTriggerEvent(new DateTime(), new DateTime()));

        WebhookAction.Result actionResult = webhookAction.execute(ctx, new Payload.Simple());
        scenario.assertResult(actionResult);
    }


    @Test @Repeat(iterations = 10)
    public void testParser() throws Exception {
        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        TransformRegistry transformRegistry = transform == null ? mock(TransformRegistry.class) : new TransformMocks.TransformRegistryMock(transform);
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_subject") : null;
        Template url = new ScriptTemplate(scriptService, "_url");
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);


        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.field(WebhookAction.Parser.URL_FIELD.getPreferredName(), url);

            if (method != null) {
                builder.field(WebhookAction.Parser.METHOD_FIELD.getPreferredName(), method.method());
            }
            if (body != null) {
                builder.field(WebhookAction.Parser.BODY_FIELD.getPreferredName(), body);
            }

            if (transform != null) {
                builder.startObject(Transform.Parser.TRANSFORM_FIELD.getPreferredName())
                        .field(transform.type(), transform);
            }
        }
        builder.endObject();

        WebhookAction.Parser actionParser = new WebhookAction.Parser(ImmutableSettings.EMPTY,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                ExecuteScenario.Success.client(), transformRegistry);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction action = actionParser.parse(parser);

        if (method != null) {
            assertThat(action.method, equalTo(method));
        } else {
            assertThat(action.method, equalTo(HttpMethod.POST));
        }

        if (body != null) {
            assertThat(action.body, equalTo(body));
        }

        assertThat(action.url, equalTo(url));

        if (transform != null) {
            assertThat(action.transform(), equalTo(transform));
        }
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SelfGenerated() throws Exception {
        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        TransformRegistry transformRegistry = transform == null ? mock(TransformRegistry.class) : new TransformMocks.TransformRegistryMock(transform);
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_body") : null;
        Template url = new ScriptTemplate(scriptService, "_url");
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT);

        WebhookAction webhookAction = new WebhookAction(logger, transform, ExecuteScenario.Success.client(), method, url, body);

        XContentBuilder builder = jsonBuilder();
        webhookAction.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = new WebhookAction.Parser(ImmutableSettings.EMPTY,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                ExecuteScenario.Success.client(), transformRegistry);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction parsedAction = actionParser.parse(parser);

        assertThat(webhookAction.body, equalTo(parsedAction.body));
        assertThat(webhookAction.method, equalTo(parsedAction.method));
        assertThat(webhookAction.url, equalTo(parsedAction.url));
        assertThat(webhookAction.transform(), equalTo(parsedAction.transform()));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SourceBuilder() throws Exception {
        Script body = randomBoolean() ? new Script("_body", ScriptService.ScriptType.INLINE, "mustache") : null;
        Script url = new Script("_url", ScriptService.ScriptType.INLINE, "mustache");
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT);
        WebhookAction.SourceBuilder sourceBuilder = new WebhookAction.SourceBuilder(url);
        sourceBuilder.method(method);
        sourceBuilder.body(body);

        XContentBuilder builder = jsonBuilder();
        sourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = new WebhookAction.Parser(ImmutableSettings.EMPTY,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                ExecuteScenario.Success.client(), null);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        Map<String, Object> emptyModel = new HashMap<>();

        WebhookAction parsedAction = actionParser.parse(parser);
        if (body != null) {
            assertThat(body.script(), equalTo(parsedAction.body.render(emptyModel)));
        } else {
            assertThat(parsedAction.body, equalTo(null));
        }
        assertThat(url.script(), equalTo(parsedAction.url.render(emptyModel)));
        assertThat(method, equalTo(parsedAction.method));
    }

    @Test(expected = ActionException.class)
    public void testParser_Failure() throws Exception {
        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        TransformRegistry transformRegistry = transform == null ? mock(TransformRegistry.class) : new TransformMocks.TransformRegistryMock(transform);
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_subject") : null;
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);


        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            if (method != null) {
                builder.field(WebhookAction.Parser.METHOD_FIELD.getPreferredName(), method.method());
            }
            if (body != null) {
                builder.field(WebhookAction.Parser.BODY_FIELD.getPreferredName(), body);
            }

            if (transform != null) {
                builder.startObject(Transform.Parser.TRANSFORM_FIELD.getPreferredName())
                        .field(transform.type(), transform);
            }
        }
        builder.endObject();

        WebhookAction.Parser actionParser = new WebhookAction.Parser(ImmutableSettings.EMPTY,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                ExecuteScenario.Success.client(), transformRegistry);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        //This should fail since we are not supplying a url
        actionParser.parse(parser);
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result() throws Exception {
        Transform.Result transformResult = randomBoolean() ? null : mock(Transform.Result.class);
        if (transformResult != null) {
            when(transformResult.type()).thenReturn("_transform_type");
            when(transformResult.payload()).thenReturn(new Payload.Simple("_key", "_value"));
        }
        TransformRegistry transformRegistry = transformResult != null ? new TransformMocks.TransformRegistryMock(transformResult) : mock(TransformRegistry.class);

        String body = "_body";
        String url = "_url";
        String reason = "_reason";
        int responseCode = randomIntBetween(200, 599);

        boolean error = randomBoolean();

        boolean success = !error && responseCode < 400;

        WebhookAction.Parser actionParser = new WebhookAction.Parser(ImmutableSettings.EMPTY,
                new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                ExecuteScenario.Success.client(), transformRegistry);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.field(Action.Result.SUCCESS_FIELD.getPreferredName(), success);
            if (!error) {
                builder.field(WebhookAction.Parser.HTTP_STATUS_FIELD.getPreferredName(), responseCode);
                builder.field(WebhookAction.Parser.BODY_FIELD.getPreferredName(), body);
                builder.field(WebhookAction.Parser.URL_FIELD.getPreferredName(), url);
                if (transformResult != null) {
                    builder.startObject("transform_result")
                            .startObject("_transform_type")
                            .field("payload", new Payload.Simple("_key", "_value").data())
                            .endObject()
                            .endObject();
                }
            } else {
                builder.field(WebhookAction.Parser.REASON_FIELD.getPreferredName(), reason);
            }
        }
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction.Result result = actionParser.parseResult(parser);

        assertThat(result.success(), equalTo(success));
        if (!error) {
            assertThat(result, instanceOf(WebhookAction.Result.Executed.class));
            WebhookAction.Result.Executed executedResult = (WebhookAction.Result.Executed) result;
            assertThat(executedResult.body(), equalTo(body));
            assertThat(executedResult.httpStatus(), equalTo(responseCode));
            assertThat(executedResult.url(), equalTo(url));
            if (transformResult != null) {
                assertThat(transformResult, equalTo(executedResult.transformResult()));
            }
        } else {
            assertThat(result, Matchers.instanceOf(WebhookAction.Result.Failure.class));
            WebhookAction.Result.Failure failedResult = (WebhookAction.Result.Failure) result;
            assertThat(failedResult.reason(), equalTo(reason));
        }
    }

    @Test @Repeat(iterations = 100)
    public void testValidUrls() throws Exception {

        HttpClient httpClient = ExecuteScenario.Success.client();
        HttpMethod method = HttpMethod.GET;

        WebhookAction webhookAction =
                new WebhookAction(logger,
                        null,
                        httpClient,
                        method,
                        new ScriptTemplate(scriptService, "http://test.com/test_{{ctx.watch_name}}"),
                        new ScriptTemplate(scriptService, TEST_BODY));

        String watchName = "test_url_encode" + randomAsciiOfLength(10);
        Watch watch = createWatch(watchName, mock(ClientProxy.class), "account1");
        WatchExecutionContext ctx = new WatchExecutionContext("testid", watch, new DateTime(), new ScheduleTriggerEvent(new DateTime(), new DateTime()));
        WebhookAction.Result result = webhookAction.execute(ctx, new Payload.Simple());
        assertThat(result, Matchers.instanceOf(WebhookAction.Result.Executed.class));
        WebhookAction.Result.Executed executed = (WebhookAction.Result.Executed)result;
        URI.create(executed.url()); //This will throw an IllegalArgumentException if the url is invalid
    }

    private Watch createWatch(String watchName, ClientProxy client, final String account) throws AddressException, IOException {
        return WatcherTestUtils.createTestWatch(watchName,
                client,
                scriptService,
                ExecuteScenario.Success.client(),
                new EmailService() {
                    @Override
                    public EmailSent send(Email email, Authentication auth, Profile profile) {
                        return new EmailSent(account, email);
                    }

                    @Override
                    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
                        return new EmailSent(account, email);
                    }
                },
                logger);
    }


    private static enum ExecuteScenario {
        ErrorCode() {
            @Override
            public HttpClient client() throws IOException {
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpMethod.class), any(String.class), any(String.class)))
                        .thenReturn(new HttpResponse(randomIntBetween(400,599)));
                return client;
            }

            @Override
            public void assertResult(WebhookAction.Result actionResult) {
                assertThat(actionResult.success(), is(false));
                assertThat(actionResult, instanceOf(WebhookAction.Result.Executed.class));
                WebhookAction.Result.Executed executedActionResult = (WebhookAction.Result.Executed) actionResult;
                assertThat(executedActionResult.httpStatus(), greaterThanOrEqualTo(400));
                assertThat(executedActionResult.httpStatus(), lessThanOrEqualTo(599));
                assertThat(executedActionResult.body(), equalTo(TEST_BODY));
                assertThat(executedActionResult.url(), equalTo(TEST_URL));
            }
        },

        Error() {
            @Override
            public HttpClient client() throws IOException {
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpMethod.class), any(String.class), any(String.class)))
                        .thenThrow(new IOException("Unable to connect"));
                return client;
            }

            @Override
            public void assertResult(WebhookAction.Result actionResult) {
                assertThat(actionResult, instanceOf(WebhookAction.Result.Failure.class));
                WebhookAction.Result.Failure failResult = (WebhookAction.Result.Failure) actionResult;
                assertThat(failResult.success(), is(false));
            }
        },

        Success() {
            @Override
            public HttpClient client() throws IOException{
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpMethod.class), any(String.class), any(String.class)))
                        .thenReturn(new HttpResponse(randomIntBetween(200,399)));
                return client;
            }

            @Override
            public void assertResult(WebhookAction.Result actionResult) {
                assertThat(actionResult, instanceOf(WebhookAction.Result.Executed.class));
                assertThat(actionResult, instanceOf(WebhookAction.Result.Executed.class));
                WebhookAction.Result.Executed executedActionResult = (WebhookAction.Result.Executed) actionResult;
                assertThat(executedActionResult.httpStatus(), greaterThanOrEqualTo(200));
                assertThat(executedActionResult.httpStatus(), lessThanOrEqualTo(399));
                assertThat(executedActionResult.body(), equalTo(TEST_BODY));
                assertThat(executedActionResult.url(), equalTo(TEST_URL));
            }
        },

        SuccessVerify() {
            @Override
            public HttpClient client() throws IOException{
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpMethod.class), eq(TEST_URL), eq(TEST_BODY)))
                        .thenReturn(new HttpResponse(200));
                when(client.execute(any(HttpMethod.class), eq(not(TEST_URL)), eq(not(TEST_BODY)))).thenThrow(new IOException("bad url or body"));
                return client;
            }

            @Override
            public void assertResult(WebhookAction.Result actionResult) {
                assertThat(actionResult, instanceOf(WebhookAction.Result.Executed.class));
                assertThat(actionResult, instanceOf(WebhookAction.Result.Executed.class));
                WebhookAction.Result.Executed executedActionResult = (WebhookAction.Result.Executed) actionResult;
                assertThat(executedActionResult.httpStatus(), equalTo(200));
                assertThat(executedActionResult.body(), equalTo(TEST_BODY));
                assertThat(executedActionResult.url(), equalTo(TEST_URL));
            }
        };

        public abstract HttpClient client() throws IOException;

        public abstract void assertResult(WebhookAction.Result result);

    }

}
