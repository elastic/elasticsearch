/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.collect.ImmutableMap;
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
import org.elasticsearch.watcher.support.http.*;
import org.elasticsearch.watcher.support.http.auth.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class WebhookActionTests extends ElasticsearchTestCase {

    static final String TEST_HOST = "test.com";

    private ThreadPool tp = null;
    private ScriptServiceProxy scriptService;
    private HttpAuthRegistry authRegistry;
    private Template testBody;
    private Template testPath;

    static final String TEST_BODY_STRING = "ERROR HAPPENED";
    static final String TEST_PATH_STRING = "/testPath";


    @Before
    public void init() throws IOException {
        tp = new ThreadPool(ThreadPool.Names.SAME);
        Settings settings = ImmutableSettings.settingsBuilder().build();
        MustacheScriptEngineService mustacheScriptEngineService = new MustacheScriptEngineService(settings);
        Set<ScriptEngineService> engineServiceSet = new HashSet<>();
        engineServiceSet.add(mustacheScriptEngineService);
        scriptService = ScriptServiceProxy.of(new ScriptService(settings, new Environment(), engineServiceSet, new ResourceWatcherService(settings, tp), new NodeSettingsService(settings)));
        testBody = new ScriptTemplate(scriptService, TEST_BODY_STRING );
        testPath = new ScriptTemplate(scriptService, TEST_PATH_STRING);
        authRegistry = new HttpAuthRegistry(ImmutableMap.of("basic", (HttpAuth.Parser) new BasicAuth.Parser()));
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

        TemplatedHttpRequest httpRequest = getTemplatedHttpRequest(method, TEST_HOST, testPath, testBody);

        WebhookAction webhookAction =
                new WebhookAction(logger,
                        transform,
                        httpClient,
                        httpRequest);

        Watch watch = createWatch("test_watch", client, account);
        WatchExecutionContext ctx = new WatchExecutionContext("testid", watch, new DateTime(), new ScheduleTriggerEvent(new DateTime(), new DateTime()));

        WebhookAction.Result actionResult = webhookAction.execute(ctx, new Payload.Simple());
        scenario.assertResult(actionResult);
    }

    private TemplatedHttpRequest getTemplatedHttpRequest(HttpMethod method, String host, Template path, Template body) {
        TemplatedHttpRequest httpRequest = new TemplatedHttpRequest();
        if (host != null) {
            httpRequest.host(host);
        }
        if (path != null) {
            httpRequest.path(path);
        }
        if (body != null) {
            httpRequest.body(body);
        }
        if (method != null) {
            httpRequest.method(method);
        }
        return httpRequest;
    }


    @Test @Repeat(iterations = 10)
    public void testParser() throws Exception {
        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        TransformRegistry transformRegistry = transform == null ? mock(TransformRegistry.class) : new TransformMocks.TransformRegistryMock(transform);
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_subject") : null;
        Template path = new ScriptTemplate(scriptService, "_url");
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);
        TemplatedHttpRequest request = getTemplatedHttpRequest(method, host, path, body);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.field(WebhookAction.Parser.REQUEST_FIELD.getPreferredName(), request);

            if (transform != null) {
                builder.startObject(Transform.Parser.TRANSFORM_FIELD.getPreferredName())
                        .field(transform.type(), transform);
            }
        }
        builder.endObject();

        WebhookAction.Parser actionParser = getParser(transformRegistry, ExecuteScenario.Success.client() );

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction action = actionParser.parse(parser);

        assertThat(action.templatedHttpRequest(), equalTo(request));
        if (transform != null) {
            assertThat(action.transform(), equalTo(transform));
        }
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SelfGenerated() throws Exception {
        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        TransformRegistry transformRegistry = transform == null ? mock(TransformRegistry.class) : new TransformMocks.TransformRegistryMock(transform);
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_body") : null;
        Template path = new ScriptTemplate(scriptService, "_url");
        String host = "test.host";

        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);

        TemplatedHttpRequest request = getTemplatedHttpRequest(method, host, path, body);
        WebhookAction webhookAction = new WebhookAction(logger, transform, ExecuteScenario.Success.client(), request);

        XContentBuilder builder = jsonBuilder();
        webhookAction.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = getParser(transformRegistry, ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction action = actionParser.parse(parser);

        assertThat(action.templatedHttpRequest(), equalTo(request));
        if (transform != null) {
            assertThat(action.transform(), equalTo(transform));
        }

    }

    @Test @Repeat(iterations = 10)
    public void testParser_SourceBuilder() throws Exception {
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_body") : null;
        Template path = new ScriptTemplate(scriptService, "_url");
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);
        TemplatedHttpRequest request = getTemplatedHttpRequest(method, host, path, body);

        WebhookAction.SourceBuilder sourceBuilder = new WebhookAction.SourceBuilder(request);

        XContentBuilder builder = jsonBuilder();
        sourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = getParser(null, ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        Map<String, Object> emptyModel = new HashMap<>();

        WebhookAction parsedAction = actionParser.parse(parser);
        assertThat(request, equalTo(parsedAction.templatedHttpRequest()));
    }

    @Test(expected = ActionException.class)
    public void testParser_Failure() throws Exception {
        final Transform transform = randomBoolean() ? null : new TransformMocks.TransformMock();
        TransformRegistry transformRegistry = transform == null ? mock(TransformRegistry.class) : new TransformMocks.TransformRegistryMock(transform);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            if (transform != null) {
                builder.startObject(Transform.Parser.TRANSFORM_FIELD.getPreferredName())
                        .field(transform.type(), transform);
            }
        }
        builder.endObject();

        WebhookAction.Parser actionParser = getParser(transformRegistry, ExecuteScenario.Success.client());

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
        String host = "test.host";
        String path = "/_url";
        String reason = "_reason";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT);

        byte[] responseBody = new byte[randomIntBetween(1,100)];
        for (int i = 0; i < responseBody.length; ++i) {
            responseBody[i] = randomByte();
        }

        HttpRequest httpRequest = new HttpRequest();
        httpRequest.host(host);
        httpRequest.body(body);
        httpRequest.path(path);
        httpRequest.method(method);
        int responseCode = randomIntBetween(200, 599);

        boolean error = randomBoolean();

        boolean success = !error && responseCode < 400;

        HttpClient client = ExecuteScenario.Success.client();

        WebhookAction.Parser actionParser = getParser(transformRegistry, client);


        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.field(Action.Result.SUCCESS_FIELD.getPreferredName(), success);
            if (!error) {
                builder.field(WebhookAction.Parser.HTTP_STATUS_FIELD.getPreferredName(), responseCode);
                builder.field(WebhookAction.Parser.RESPONSE_BODY.getPreferredName(), responseBody);
                builder.field(WebhookAction.Parser.REQUEST_FIELD.getPreferredName(), httpRequest);
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
            assertThat(executedResult.responseBody(), equalTo(responseBody));
            assertThat(executedResult.httpStatus(), equalTo(responseCode));
            assertThat(executedResult.httpRequest(), equalTo(httpRequest));
            if (transformResult != null) {
                assertThat(transformResult, equalTo(executedResult.transformResult()));
            }
        } else {
            assertThat(result, Matchers.instanceOf(WebhookAction.Result.Failure.class));
            WebhookAction.Result.Failure failedResult = (WebhookAction.Result.Failure) result;
            assertThat(failedResult.reason(), equalTo(reason));
        }
    }

    private WebhookAction.Parser getParser(TransformRegistry transformRegistry, HttpClient client) {
        return new WebhookAction.Parser(ImmutableSettings.EMPTY,
                    client, transformRegistry, new HttpRequest.Parser(authRegistry),
                    new TemplatedHttpRequest.Parser(new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                            authRegistry) );
    }

    @Test @Repeat(iterations = 100)
    public void testValidUrls() throws Exception {

        HttpClient httpClient = ExecuteScenario.Success.client();
        HttpMethod method = HttpMethod.POST;
        Template path = new ScriptTemplate(scriptService, "/test_{{ctx.watch_name}}");
        String host = "test.host";
        TemplatedHttpRequest templatedHttpRequest = getTemplatedHttpRequest(method, host, path, testBody);

        WebhookAction webhookAction =
                new WebhookAction(logger,
                        null,
                        httpClient,
                        templatedHttpRequest);



        String watchName = "test_url_encode" + randomAsciiOfLength(10);
        Watch watch = createWatch(watchName, mock(ClientProxy.class), "account1");
        WatchExecutionContext ctx = new WatchExecutionContext("testid", watch, new DateTime(), new ScheduleTriggerEvent(new DateTime(), new DateTime()));
        WebhookAction.Result result = webhookAction.execute(ctx, new Payload.Simple());
        assertThat(result, Matchers.instanceOf(WebhookAction.Result.Executed.class));
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
                when(client.execute(any(HttpRequest.class)))
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
                assertThat(executedActionResult.httpRequest().body(), equalTo(TEST_BODY_STRING));
                assertThat(executedActionResult.httpRequest().path(), equalTo(TEST_PATH_STRING));
            }
        },

        Error() {
            @Override
            public HttpClient client() throws IOException {
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpRequest.class)))
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
                when(client.execute(any(HttpRequest.class)))
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
                assertThat(executedActionResult.httpRequest().body(), equalTo(TEST_BODY_STRING));
                assertThat(executedActionResult.httpRequest().path(), equalTo(TEST_PATH_STRING));
            }
        };

        public abstract HttpClient client() throws IOException;

        public abstract void assertResult(WebhookAction.Result result);

    }

}
