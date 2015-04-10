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
import org.elasticsearch.watcher.actions.email.service.*;
import org.elasticsearch.watcher.support.http.*;
import org.elasticsearch.watcher.support.http.auth.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.template.ScriptTemplate;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.test.WatcherTestUtils;
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
import java.util.HashSet;
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
    static final int TEST_PORT = 8089;

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

        final String account = "account1";

        TemplatedHttpRequest httpRequest = getTemplatedHttpRequest(method, TEST_HOST, TEST_PORT, testPath, testBody);

        WebhookAction webhookAction = new WebhookAction(logger, httpClient, httpRequest);

        Watch watch = createWatch("test_watch", client, account);
        WatchExecutionContext ctx = new WatchExecutionContext("testid", watch, new DateTime(), new ScheduleTriggerEvent(new DateTime(), new DateTime()));

        WebhookAction.Result actionResult = webhookAction.execute("_id", ctx, new Payload.Simple());
        scenario.assertResult(actionResult);
    }

    private TemplatedHttpRequest.SourceBuilder getTemplatedHttpRequestSourceBuilder(HttpMethod method, String host, int port, Template path, Template body) {
        TemplatedHttpRequest.SourceBuilder httpRequest = new TemplatedHttpRequest.SourceBuilder(host, port);
        if (path != null) {
            httpRequest.setPath(path);
        }
        if (body != null) {
            httpRequest.setBody(body);
        }
        if (method != null) {
            httpRequest.setMethod(method);
        }
        return httpRequest;
    }

    private TemplatedHttpRequest getTemplatedHttpRequest(HttpMethod method, String host, int port, Template path, Template body) {
        TemplatedHttpRequest httpRequest = new TemplatedHttpRequest();
        httpRequest.host(host);
        httpRequest.port(port);
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
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_subject") : null;
        Template path = new ScriptTemplate(scriptService, "_url");
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);
        TemplatedHttpRequest request = getTemplatedHttpRequest(method, host, TEST_PORT, path, body);

        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = getParser(ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction action = actionParser.parse(parser);

        assertThat(action.templatedHttpRequest(), equalTo(request));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SelfGenerated() throws Exception {
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_body") : null;
        Template path = new ScriptTemplate(scriptService, "_url");
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);

        TemplatedHttpRequest request = getTemplatedHttpRequest(method, host, TEST_PORT, path, body);
        WebhookAction webhookAction = new WebhookAction(logger, ExecuteScenario.Success.client(), request);

        XContentBuilder builder = jsonBuilder();
        webhookAction.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = getParser(ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction action = actionParser.parse(parser);

        assertThat(action.templatedHttpRequest(), equalTo(request));
    }

    @Test //@Repeat(iterations = 10)
    public void testParser_SourceBuilder() throws Exception {
        Template body = randomBoolean() ? new ScriptTemplate(scriptService, "_body") : null;
        Template path = new ScriptTemplate(scriptService, "_url");
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, null);
        TemplatedHttpRequest.SourceBuilder request = getTemplatedHttpRequestSourceBuilder(method, host, TEST_PORT, path, body);

        WebhookAction.SourceBuilder sourceBuilder = new WebhookAction.SourceBuilder("_id", request);

        XContentBuilder builder = jsonBuilder();
        sourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookAction.Parser actionParser = getParser(ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), is(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), is(WebhookAction.TYPE));
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
        WebhookAction parsedAction = actionParser.parse(parser);
        assertThat(parsedAction.templatedHttpRequest().host(), equalTo(host));
        assertThat(parsedAction.templatedHttpRequest().port(), equalTo(TEST_PORT));
        assertThat(parsedAction.templatedHttpRequest().path(), equalTo(path));
        assertThat(parsedAction.templatedHttpRequest().body(), equalTo(body));
    }

    @Test(expected = ActionException.class)
    public void testParser_Failure() throws Exception {

        XContentBuilder builder = jsonBuilder().startObject().endObject();
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookAction.Parser actionParser = getParser(ExecuteScenario.Success.client());
        //This should fail since we are not supplying a url
        actionParser.parse(parser);
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result() throws Exception {
        String body = "_body";
        String host = "test.host";
        String path = "/_url";
        String reason = "_reason";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT);

        HttpRequest request = new HttpRequest();
        request.host(host);
        request.body(body);
        request.path(path);
        request.method(method);

        HttpResponse response = new HttpResponse(randomIntBetween(200, 599), randomAsciiOfLength(10).getBytes(UTF8));

        boolean error = randomBoolean();

        boolean success = !error && response.status() < 400;

        HttpClient client = ExecuteScenario.Success.client();

        WebhookAction.Parser actionParser = getParser(client);


        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.field(Action.Result.SUCCESS_FIELD.getPreferredName(), success);
            if (!error) {
                builder.field(WebhookAction.Parser.REQUEST_FIELD.getPreferredName(), request);
                builder.field(WebhookAction.Parser.RESPONSE_FIELD.getPreferredName(), response);
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
            assertThat(executedResult.request(), equalTo(request));
            assertThat(executedResult.response(), equalTo(response));
        } else {
            assertThat(result, Matchers.instanceOf(WebhookAction.Result.Failure.class));
            WebhookAction.Result.Failure failedResult = (WebhookAction.Result.Failure) result;
            assertThat(failedResult.reason(), equalTo(reason));
        }
    }

    private WebhookAction.Parser getParser(HttpClient client) {
        return new WebhookAction.Parser(ImmutableSettings.EMPTY,
                    client, new HttpRequest.Parser(authRegistry),
                    new TemplatedHttpRequest.Parser(new ScriptTemplate.Parser(ImmutableSettings.EMPTY, scriptService),
                            authRegistry) );
    }

    @Test @Repeat(iterations = 100)
    public void testValidUrls() throws Exception {

        HttpClient httpClient = ExecuteScenario.Success.client();
        HttpMethod method = HttpMethod.POST;
        Template path = new ScriptTemplate(scriptService, "/test_{{ctx.watch_id}}");
        String host = "test.host";
        TemplatedHttpRequest templatedHttpRequest = getTemplatedHttpRequest(method, host, TEST_PORT, path, testBody);

        WebhookAction webhookAction = new WebhookAction(logger, httpClient, templatedHttpRequest);

        String watchName = "test_url_encode" + randomAsciiOfLength(10);
        Watch watch = createWatch(watchName, mock(ClientProxy.class), "account1");
        WatchExecutionContext ctx = new WatchExecutionContext("testid", watch, new DateTime(), new ScheduleTriggerEvent(new DateTime(), new DateTime()));
        WebhookAction.Result result = webhookAction.execute("_id", ctx, new Payload.Simple());
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


    private enum ExecuteScenario {
        ErrorCode() {
            @Override
            public HttpClient client() throws IOException {
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(randomIntBetween(400, 599)));
                return client;
            }

            @Override
            public void assertResult(WebhookAction.Result actionResult) {
                assertThat(actionResult.success(), is(false));
                assertThat(actionResult, instanceOf(WebhookAction.Result.Executed.class));
                WebhookAction.Result.Executed executedActionResult = (WebhookAction.Result.Executed) actionResult;
                assertThat(executedActionResult.response().status(), greaterThanOrEqualTo(400));
                assertThat(executedActionResult.response().status(), lessThanOrEqualTo(599));
                assertThat(executedActionResult.request().body(), equalTo(TEST_BODY_STRING));
                assertThat(executedActionResult.request().path(), equalTo(TEST_PATH_STRING));
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
                assertThat(executedActionResult.response().status(), greaterThanOrEqualTo(200));
                assertThat(executedActionResult.response().status(), lessThanOrEqualTo(399));
                assertThat(executedActionResult.request().body(), equalTo(TEST_BODY_STRING));
                assertThat(executedActionResult.request().path(), equalTo(TEST_PATH_STRING));
            }
        };

        public abstract HttpClient client() throws IOException;

        public abstract void assertResult(WebhookAction.Result result);

    }

}
