/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.webhook;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.Action.Result.Status;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.actions.email.EmailActionTests;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.notification.WebhookService;
import org.elasticsearch.xpack.watcher.notification.email.Attachment;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import javax.mail.internet.AddressException;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.common.http.HttpClientTests.mockClusterService;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebhookActionTests extends ESTestCase {

    private static final String TEST_HOST = "test.com";
    private static final int TEST_PORT = 8089;
    private static final String TEST_BODY_STRING = "ERROR HAPPENED";
    private static final String TEST_PATH_STRING = "/testPath";

    private TextTemplateEngine templateEngine;
    private TextTemplate testBody;
    private TextTemplate testPath;

    @Before
    public void init() throws Exception {
        templateEngine = new MockTextTemplateEngine();
        testBody = new TextTemplate(TEST_BODY_STRING);
        testPath = new TextTemplate(TEST_PATH_STRING);
    }

    public void testExecute() throws Exception {
        ExecuteScenario scenario = randomFrom(ExecuteScenario.Success, ExecuteScenario.ErrorCode);

        HttpClient httpClient = scenario.client();
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD);

        HttpRequestTemplate httpRequest = getHttpRequestTemplate(method, TEST_HOST, TEST_PORT, testPath, testBody, null);

        WebhookAction action = new WebhookAction(httpRequest);
        WebhookService webhookService = new WebhookService(
            Settings.EMPTY,
            httpClient,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, webhookService, templateEngine);
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", new Payload.Simple("foo", "bar"));

        Action.Result actionResult = executable.execute("_id", ctx, Payload.EMPTY);

        scenario.assertResult(httpClient, actionResult);
    }

    private HttpRequestTemplate getHttpRequestTemplate(
        HttpMethod method,
        String host,
        int port,
        TextTemplate path,
        TextTemplate body,
        Map<String, TextTemplate> params
    ) {
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
        if (params != null) {
            builder.putParams(params);
        }
        return builder.build();
    }

    public void testParser() throws Exception {
        TextTemplate body = randomBoolean() ? new TextTemplate("_subject") : null;
        TextTemplate path = new TextTemplate("_url");
        String host = "test.host";
        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD, null);
        HttpRequestTemplate request = getHttpRequestTemplate(method, host, TEST_PORT, path, body, null);

        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());

        XContentParser parser = createParser(builder);
        parser.nextToken();

        ExecutableWebhookAction executable = actionParser.parseExecutable(randomAlphaOfLength(5), randomAlphaOfLength(5), parser);

        assertThat(executable.action().getRequest(), equalTo(request));
    }

    public void testParserSelfGenerated() throws Exception {
        TextTemplate body = randomBoolean() ? new TextTemplate("_body") : null;
        TextTemplate path = new TextTemplate("_url");
        String host = "test.host";
        String watchId = "_watch";
        String actionId = randomAlphaOfLength(5);

        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD, null);

        HttpRequestTemplate request = getHttpRequestTemplate(method, host, TEST_PORT, path, body, null);
        WebhookAction action = new WebhookAction(request);
        WebhookService webhookService = new WebhookService(
            Settings.EMPTY,
            ExecuteScenario.Success.client(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, webhookService, templateEngine);

        XContentBuilder builder = jsonBuilder();
        executable.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());

        XContentParser parser = createParser(builder);
        parser.nextToken();

        ExecutableWebhookAction parsedExecutable = actionParser.parseExecutable(watchId, actionId, parser);
        assertThat(parsedExecutable, notNullValue());
        assertThat(parsedExecutable.action(), is(action));
    }

    public void testParserBuilder() throws Exception {
        TextTemplate body = randomBoolean() ? new TextTemplate("_body") : null;
        TextTemplate path = new TextTemplate("_url");
        String host = "test.host";

        String watchId = "_watch";
        String actionId = randomAlphaOfLength(5);

        HttpMethod method = randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD, null);
        HttpRequestTemplate request = getHttpRequestTemplate(method, host, TEST_PORT, path, body, null);

        WebhookAction action = WebhookAction.builder(request).build();

        XContentBuilder builder = jsonBuilder();
        action.toXContent(builder, ToXContent.EMPTY_PARAMS);

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());

        XContentParser parser = createParser(builder);
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

        XContentParser parser = createParser(builder);
        parser.nextToken();

        WebhookActionFactory actionParser = webhookFactory(ExecuteScenario.Success.client());
        // This should fail since we are not supplying a url
        try {
            actionParser.parseExecutable("_watch", randomAlphaOfLength(5), parser);
            fail("expected a WebhookActionException since we only provided either a host or a port but not both");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed parsing http request template"));
        }
    }

    private WebhookActionFactory webhookFactory(HttpClient client) {
        WebhookService webhookService = new WebhookService(
            Settings.EMPTY,
            client,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        return new WebhookActionFactory(webhookService, templateEngine);
    }

    public void testThatSelectingProxyWorks() throws Exception {
        Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());

        try (
            HttpClient httpClient = new HttpClient(Settings.EMPTY, new SSLService(environment), null, mockClusterService());
            MockWebServer proxyServer = new MockWebServer()
        ) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody("fullProxiedContent"));

            HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", 65535)
                .path("/")
                .proxy(new HttpProxy("localhost", proxyServer.getPort()));
            WebhookAction action = new WebhookAction(builder.build());

            WebhookService webhookService = new WebhookService(
                Settings.EMPTY,
                httpClient,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, webhookService, templateEngine);
            String watchId = "test_url_encode" + randomAlphaOfLength(10);
            ScheduleTriggerEvent triggerEvent = new ScheduleTriggerEvent(
                watchId,
                ZonedDateTime.now(ZoneOffset.UTC),
                ZonedDateTime.now(ZoneOffset.UTC)
            );
            TriggeredExecutionContext ctx = new TriggeredExecutionContext(
                watchId,
                ZonedDateTime.now(ZoneOffset.UTC),
                triggerEvent,
                timeValueSeconds(5)
            );
            Watch watch = createWatch(watchId);
            ctx.ensureWatchExists(() -> watch);
            executable.execute("_id", ctx, new Payload.Simple());

            assertThat(proxyServer.requests(), hasSize(1));
        }
    }

    public void testValidUrls() throws Exception {
        HttpClient client = mock(HttpClient.class);
        when(client.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(randomIntBetween(200, 399)));
        String watchId = "test_url_encode" + randomAlphaOfLength(10);

        HttpMethod method = HttpMethod.POST;
        TextTemplate path = new TextTemplate("/test_" + watchId);
        String host = "test.host";
        TextTemplate testBody = new TextTemplate("ERROR HAPPENED");
        HttpRequestTemplate requestTemplate = getHttpRequestTemplate(method, host, TEST_PORT, path, testBody, null);
        WebhookAction action = new WebhookAction(requestTemplate);

        WebhookService webhookService = new WebhookService(
            Settings.EMPTY,
            client,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ExecutableWebhookAction executable = new ExecutableWebhookAction(action, logger, webhookService, templateEngine);

        ScheduleTriggerEvent triggerEvent = new ScheduleTriggerEvent(
            watchId,
            ZonedDateTime.now(ZoneOffset.UTC),
            ZonedDateTime.now(ZoneOffset.UTC)
        );
        TriggeredExecutionContext ctx = new TriggeredExecutionContext(
            watchId,
            ZonedDateTime.now(ZoneOffset.UTC),
            triggerEvent,
            timeValueSeconds(5)
        );
        Watch watch = createWatch(watchId);
        ctx.ensureWatchExists(() -> watch);
        Action.Result result = executable.execute("_id", ctx, new Payload.Simple());
        assertThat(result, Matchers.instanceOf(WebhookAction.Result.Success.class));
    }

    private Watch createWatch(String watchId) throws AddressException, IOException {
        return WatcherTestUtils.createTestWatch(
            watchId,
            mock(Client.class),
            ExecuteScenario.Success.client(),
            new EmailActionTests.NoopEmailService(),
            mock(WatcherSearchTemplateService.class),
            logger
        );
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
                when(client.execute(any(HttpRequest.class))).thenThrow(new IOException("Unable to connect"));
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
            public HttpClient client() throws IOException {
                HttpClient client = mock(HttpClient.class);
                when(client.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(randomIntBetween(200, 399)));
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
            public HttpClient client() throws IOException {
                return mock(HttpClient.class);
            }

            @Override
            public void assertResult(HttpClient client, Action.Result actionResult) throws Exception {
                verify(client, never()).execute(any(HttpRequest.class));
            }
        };

        public abstract HttpClient client() throws IOException;

        public abstract void assertResult(HttpClient client, Action.Result result) throws Exception;
    }
}
