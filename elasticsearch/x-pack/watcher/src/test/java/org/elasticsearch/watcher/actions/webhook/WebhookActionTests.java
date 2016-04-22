/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Action.Result.Status;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpMethod;
import org.elasticsearch.watcher.support.http.HttpProxy;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.test.MockTextTemplateEngine;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.xpack.notification.email.Attachment;
import org.elasticsearch.xpack.notification.email.Authentication;
import org.elasticsearch.xpack.notification.email.Email;
import org.elasticsearch.xpack.notification.email.Profile;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Before;

import javax.mail.internet.AddressException;
import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 */
public class WebhookActionTests extends ESTestCase {

    static final String TEST_HOST = "test.com";
    static final int TEST_PORT = 8089;

    private TextTemplateEngine templateEngine;
    private HttpAuthRegistry authRegistry;
    private TextTemplate testBody;
    private TextTemplate testPath;

    static final String TEST_BODY_STRING = "ERROR HAPPENED";
    static final String TEST_PATH_STRING = "/testPath";


    @Before
    public void init() throws Exception {
        templateEngine = new MockTextTemplateEngine();
        SecretService secretService = mock(SecretService.class);
        testBody = TextTemplate.inline(TEST_BODY_STRING).build();
        testPath = TextTemplate.inline(TEST_PATH_STRING).build();
        authRegistry = new HttpAuthRegistry(singletonMap("basic", new BasicAuthFactory(secretService)));
    }

    public void testExecute() throws Exception {
        ExecuteScenario scenario = randomFrom(ExecuteScenario.Success, ExecuteScenario.ErrorCode);

        HttpClient httpClient = scenario.client();
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD);

        HttpRequestTemplate httpRequest = getHttpRequestTemplate(method, TEST_HOST, TEST_PORT, testPath, testBody, null);

        WebhookAction action = new WebhookAction(httpRequest);
        ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, httpClient, templateEngine);
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", new Payload.Simple("foo", "bar"));

        Action.Result actionResult = executable.execute("_id", ctx, Payload.EMPTY);

        scenario.assertResult(httpClient, actionResult);
    }

    private HttpRequestTemplate getHttpRequestTemplate(HttpMethod method, String host, int port, TextTemplate path, TextTemplate body,
                                                       Map<String, TextTemplate> params) {
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder(host, port);
        if (path != null) {
            builder.path(path);
        }
        if (body != null) {
            builder.body(body);
        }
        if (method != null) {
            builder.method(method);
        }
        if (params != null){
            builder.putParams(params);
        }
        return builder.build();
    }

    public void testParser() throws Exception {
        TextTemplate body = randomBoolean() ? TextTemplate.inline("_subject").build() : null;
        TextTemplate path = TextTemplate.inline("_url").build();
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD, null);
        HttpRequestTemplate request = getHttpRequestTemplate(method, host, TEST_PORT, path, body, null);

        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        ExecutableWebhookAction executable = actionParser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(5), parser);

        assertThat(executable.action().getRequest(), equalTo(request));
    }

    public void testParserSelfGenerated() throws Exception {
        TextTemplate body = randomBoolean() ? TextTemplate.inline("_body").build() : null;
        TextTemplate path = TextTemplate.inline("_url").build();
        String host = "test.host";
        String watchId = "_watch";
        String actionId = randomAsciiOfLength(5);

        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD, null);

        HttpRequestTemplate request = getHttpRequestTemplate(method, host, TEST_PORT, path, body, null);
        WebhookAction action = new WebhookAction(request);
        ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, ExecuteScenario.Success.client(), templateEngine);

        XContentBuilder builder = jsonBuilder();
        executable.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        ExecutableWebhookAction parsedExecutable = actionParser.parseExecutable(watchId, actionId, parser);
        assertThat(parsedExecutable, notNullValue());
        assertThat(parsedExecutable.action(), is(action));
    }

    public void testParserBuilder() throws Exception {
        TextTemplate body = randomBoolean() ? TextTemplate.inline("_body").build() : null;
        TextTemplate path = TextTemplate.inline("_url").build();
        String host = "test.host";

        String watchId = "_watch";
        String actionId = randomAsciiOfLength(5);

        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD,  null);
        HttpRequestTemplate request = getHttpRequestTemplate(method, host, TEST_PORT, path, body, null);

        WebhookAction action = WebhookAction.builder(request).build();

        XContentBuilder builder = jsonBuilder();
        action.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
        ExecutableWebhookAction parsedAction = actionParser.parseExecutable(watchId, actionId, parser);
        assertThat(parsedAction.action(), is(action));
    }

    public void testParserFailure() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject();
        if (randomBoolean()) {
            builder.field(HttpRequest.Field.HOST.getPreferredName(), TEST_HOST);
        } else {
            builder.field(HttpRequest.Field.PORT.getPreferredName(), TEST_PORT);
        }
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());
        //This should fail since we are not supplying a url
        try {
            actionParser.parseExecutable("_watch", randomAsciiOfLength(5), parser);
            fail("expected a WebhookActionException since we only provided either a host or a port but not both");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed parsing http request template"));
        }
    }

    private WebhookActionFactory webhookFactory(HttpClient client) {
        return new WebhookActionFactory(Settings.EMPTY, client, new HttpRequestTemplate.Parser(authRegistry), templateEngine);
    }

    public void testThatSelectingProxyWorks() throws Exception {
        Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        HttpClient httpClient = new HttpClient(Settings.EMPTY, authRegistry, environment).start();

        MockWebServer proxyServer = new MockWebServer();
        try {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody("fullProxiedContent"));

            HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", 65535)
                    .path("/").proxy(new HttpProxy("localhost", proxyServer.getPort()));
            WebhookAction action = new WebhookAction(builder.build());

            ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, httpClient, templateEngine);
            String watchId = "test_url_encode" + randomAsciiOfLength(10);
            Watch watch = createWatch(watchId, mock(WatcherClientProxy.class), "account1");
            WatchExecutionContext ctx = new TriggeredExecutionContext(watch, new DateTime(UTC),
                    new ScheduleTriggerEvent(watchId, new DateTime(UTC), new DateTime(UTC)), timeValueSeconds(5));
            executable.execute("_id", ctx, new Payload.Simple());

            assertThat(proxyServer.getRequestCount(), is(1));
        } finally {
            proxyServer.shutdown();
        }
    }

    public void testValidUrls() throws Exception {
        HttpClient client = mock(HttpClient.class);
        when(client.execute(any(HttpRequest.class)))
                .thenReturn(new HttpResponse(randomIntBetween(200, 399)));
        String watchId = "test_url_encode" + randomAsciiOfLength(10);

        HttpMethod method = HttpMethod.POST;
        TextTemplate path = TextTemplate.defaultType("/test_" + watchId).build();
        String host = "test.host";
        TextTemplate testBody = TextTemplate.inline("ERROR HAPPENED").build();
        HttpRequestTemplate requestTemplate = getHttpRequestTemplate(method, host, TEST_PORT, path, testBody, null);
        WebhookAction action = new WebhookAction(requestTemplate);

        ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, client, templateEngine);

        Watch watch = createWatch(watchId, mock(WatcherClientProxy.class), "account1");
        WatchExecutionContext ctx = new TriggeredExecutionContext(watch, new DateTime(UTC),
                new ScheduleTriggerEvent(watchId, new DateTime(UTC), new DateTime(UTC)), timeValueSeconds(5));
        Action.Result result = executable.execute("_id", ctx, new Payload.Simple());
        assertThat(result, Matchers.instanceOf(WebhookAction.Result.Success.class));
    }

    private Watch createWatch(String watchId, WatcherClientProxy client, final String account) throws AddressException, IOException {
        return WatcherTestUtils.createTestWatch(watchId,
                client,
                ExecuteScenario.Success.client(),
                new AbstractWatcherIntegrationTestCase.NoopEmailService() {
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
    };

    private enum ExecuteScenario {
        ErrorCode() {
            @Override
            public HttpClient client() throws IOException {
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(randomIntBetween(400, 599)));
                return client;
            }

            @Override
            public void assertResult(HttpClient client, Action.Result actionResult) throws Exception {
                assertThat(actionResult.status(), is(Status.FAILURE));
                assertThat(actionResult, instanceOf(WebhookAction.Result.Failure.class));
                WebhookAction.Result.Failure executedActionResult = (WebhookAction.Result.Failure) actionResult;
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
            public void assertResult(HttpClient client, Action.Result actionResult) throws Exception {
                assertThat(actionResult, instanceOf(WebhookAction.Result.Failure.class));
                WebhookAction.Result.Failure failResult = (WebhookAction.Result.Failure) actionResult;
                assertThat(failResult.status(), is(Status.FAILURE));
            }
        },

        Success() {
            @Override
            public HttpClient client() throws IOException{
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpRequest.class)))
                        .thenReturn(new HttpResponse(randomIntBetween(200, 399)));
                return client;
            }

            @Override
            public void assertResult(HttpClient client, Action.Result actionResult) throws Exception {
                assertThat(actionResult.status(), is(Status.SUCCESS));
                assertThat(actionResult, instanceOf(WebhookAction.Result.Success.class));
                WebhookAction.Result.Success executedActionResult = (WebhookAction.Result.Success) actionResult;
                assertThat(executedActionResult.response().status(), greaterThanOrEqualTo(200));
                assertThat(executedActionResult.response().status(), lessThanOrEqualTo(399));
                assertThat(executedActionResult.request().body(), equalTo(TEST_BODY_STRING));
                assertThat(executedActionResult.request().path(), equalTo(TEST_PATH_STRING));
            }
        },

        NoExecute() {
            @Override
            public HttpClient client() throws IOException{
                return mock(HttpClient.class);
            }

            @Override
            public void assertResult(HttpClient client, Action.Result actionResult) throws Exception {
                verify(client, never()).execute(any(HttpRequest.class));
            }
        };

        public abstract HttpClient client() throws IOException;

        public abstract void assertResult(HttpClient client, Action.Result result) throws Exception ;
    }
}
