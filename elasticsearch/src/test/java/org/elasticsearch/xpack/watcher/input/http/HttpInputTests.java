/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.http;

import io.netty.handler.codec.http.HttpHeaders;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpContentType;
import org.elasticsearch.xpack.common.http.HttpMethod;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.http.HttpResponse;
import org.elasticsearch.xpack.common.http.Scheme;
import org.elasticsearch.xpack.common.http.auth.HttpAuth;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuth;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.input.InputBuilders;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpInputTests extends ESTestCase {
    private HttpClient httpClient;
    private HttpInputFactory httpParser;
    private TextTemplateEngine templateEngine;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
        templateEngine = mock(TextTemplateEngine.class);
        HttpAuthRegistry registry = new HttpAuthRegistry(singletonMap("basic", new BasicAuthFactory(null)));
        httpParser = new HttpInputFactory(Settings.EMPTY, httpClient, templateEngine, new HttpRequestTemplate.Parser(registry));
    }

    public void testExecute() throws Exception {
        String host = "_host";
        int port = 123;
        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder(host, port)
                .method(HttpMethod.POST)
                .body("_body");
        HttpInput httpInput;

        HttpResponse response;
        switch (randomIntBetween(1, 6)) {
            case 1:
                response = new HttpResponse(123, "{\"key\" : \"value\"}".getBytes(StandardCharsets.UTF_8));
                httpInput = InputBuilders.httpInput(request.build()).build();
                break;
            case 2:
                response = new HttpResponse(123, "---\nkey : value".getBytes(StandardCharsets.UTF_8));
                httpInput = InputBuilders.httpInput(request.build()).expectedResponseXContentType(HttpContentType.YAML).build();
                break;
            case 3:
                response = new HttpResponse(123, "{\"key\" : \"value\"}".getBytes(StandardCharsets.UTF_8),
                    singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { XContentType.JSON.mediaType() }));
                httpInput = InputBuilders.httpInput(request.build()).build();
                break;
            case 4:
                response = new HttpResponse(123, "key: value".getBytes(StandardCharsets.UTF_8),
                        singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { XContentType.YAML.mediaType() }));
                httpInput = InputBuilders.httpInput(request.build()).build();
                break;
            case 5:
                response = new HttpResponse(123, "---\nkey: value".getBytes(StandardCharsets.UTF_8),
                        singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { "unrecognized_content_type" }));
                httpInput = InputBuilders.httpInput(request.build()).expectedResponseXContentType(HttpContentType.YAML).build();
                break;
            default:
                response = new HttpResponse(123, "{\"key\" : \"value\"}".getBytes(StandardCharsets.UTF_8),
                        singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { "unrecognized_content_type" }));
                httpInput = InputBuilders.httpInput(request.build()).build();
                break;
        }

        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, logger, httpClient, templateEngine);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);
        when(templateEngine.render(eq(new TextTemplate("_body")), any(Map.class))).thenReturn("_body");

        WatchExecutionContext ctx = createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.payload().data(), hasEntry("key", "value"));
    }

    public void testExecuteNonJson() throws Exception {
        String host = "_host";
        int port = 123;
        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder(host, port)
                .method(HttpMethod.POST)
                .body("_body");
        HttpInput httpInput = InputBuilders.httpInput(request.build()).expectedResponseXContentType(HttpContentType.TEXT).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, logger, httpClient, templateEngine);
        String notJson = "This is not json";
        HttpResponse response = new HttpResponse(123, notJson.getBytes(StandardCharsets.UTF_8));
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);
        when(templateEngine.render(eq(new TextTemplate("_body")), any(Map.class))).thenReturn("_body");

        WatchExecutionContext ctx = createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.payload().data().get("_value").toString(), equalTo(notJson));
    }

    public void testParser() throws Exception {
        final HttpMethod httpMethod = rarely() ? null : randomFrom(HttpMethod.values());
        Scheme scheme = randomFrom(Scheme.HTTP, Scheme.HTTPS, null);
        String host = randomAsciiOfLength(3);
        int port = randomIntBetween(8000, 9000);
        String path = randomAsciiOfLength(3);
        TextTemplate pathTemplate = new TextTemplate(path);
        String body = randomBoolean() ? randomAsciiOfLength(3) : null;
        Map<String, TextTemplate> params =
                randomBoolean() ? new MapBuilder<String, TextTemplate>().put("a", new TextTemplate("b")).map() : null;
        Map<String, TextTemplate> headers =
                randomBoolean() ? new MapBuilder<String, TextTemplate>().put("c", new TextTemplate("d")).map() : null;
        HttpAuth auth = randomBoolean() ? new BasicAuth("username", "password".toCharArray()) : null;
        HttpRequestTemplate.Builder requestBuilder = HttpRequestTemplate.builder(host, port)
                .scheme(scheme)
                .method(httpMethod)
                .path(pathTemplate)
                .body(body != null ? new TextTemplate(body) : null)
                .auth(auth);

        if (params != null) {
            requestBuilder.putParams(params);
        }
        if (headers != null) {
            requestBuilder.putHeaders(headers);
        }
        HttpInput.Builder inputBuilder = InputBuilders.httpInput(requestBuilder);
        HttpContentType expectedResponseXContentType = randomFrom(HttpContentType.values());

        String[] extractKeys = randomFrom(new String[]{"foo", "bar"}, new String[]{"baz"}, null);
        if (expectedResponseXContentType != HttpContentType.TEXT) {
            if (extractKeys != null) {
                inputBuilder.extractKeys(extractKeys);
            }
        }

        inputBuilder.expectedResponseXContentType(expectedResponseXContentType);
        XContentBuilder source = jsonBuilder().value(inputBuilder.build());
        XContentParser parser = createParser(source);
        parser.nextToken();
        HttpInput result = httpParser.parseInput("_id", parser);

        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.getRequest().scheme(), equalTo(scheme != null ? scheme : Scheme.HTTP)); // http is the default
        assertThat(result.getRequest().method(), equalTo(httpMethod != null ? httpMethod : HttpMethod.GET)); // get is the default
        assertThat(result.getRequest().host(), equalTo(host));
        assertThat(result.getRequest().port(), equalTo(port));
        assertThat(result.getRequest().path(), is(new TextTemplate(path)));
        assertThat(result.getExpectedResponseXContentType(), equalTo(expectedResponseXContentType));
        if (expectedResponseXContentType != HttpContentType.TEXT && extractKeys != null) {
            for (String key : extractKeys) {
                assertThat(result.getExtractKeys().contains(key), is(true));
            }
        }
        if (params != null) {
            assertThat(result.getRequest().params(), hasEntry(is("a"), is(new TextTemplate("b"))));
        }
        if (headers != null) {
            assertThat(result.getRequest().headers(), hasEntry(is("c"), is(new TextTemplate("d"))));
        }
        assertThat(result.getRequest().auth(), equalTo(auth));
        if (body != null) {
            assertThat(result.getRequest().body(), is(new TextTemplate(body)));
        } else {
            assertThat(result.getRequest().body(), nullValue());
        }
    }

    public void testParserInvalidHttpMethod() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .startObject("request")
                    .field("method", "_method")
                    .field("body", "_body")
                .endObject()
                .endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            httpParser.parseInput("_id", parser);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("unsupported http method [_METHOD]"));
        }
    }

    public void testThatHeadersAreIncludedInPayload() throws Exception {
        String headerName = randomAsciiOfLength(10);
        String headerValue = randomAsciiOfLength(10);
        boolean responseHasContent = randomBoolean();

        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder("localhost", 8080);
        HttpInput httpInput = InputBuilders.httpInput(request.build()).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, logger, httpClient, templateEngine);

        Map<String, String[]> responseHeaders = new HashMap<>();
        responseHeaders.put(headerName, new String[] { headerValue });
        HttpResponse response;
        if (responseHasContent) {
            response = new HttpResponse(200, "body".getBytes(StandardCharsets.UTF_8), responseHeaders);
        } else {
            BytesReference bytesReference = null;
            response = new HttpResponse(200, bytesReference, responseHeaders);
        }

        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        when(templateEngine.render(eq(new TextTemplate("_body")), any(Map.class))).thenReturn("_body");

        WatchExecutionContext ctx = createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());

        assertThat(result.type(), equalTo(HttpInput.TYPE));
        List<String> expectedHeaderValues = new ArrayList<>();
        expectedHeaderValues.add(headerValue);
        Map<String, Object> expectedHeaderMap = MapBuilder.<String, Object>newMapBuilder()
                .put(headerName.toLowerCase(Locale.ROOT), expectedHeaderValues)
                .map();
        assertThat(result.payload().data(), hasKey("_headers"));
        assertThat(result.payload().data().get("_headers"), equalTo(expectedHeaderMap));
    }

    public void testThatExpectedContentTypeOverridesReturnedContentType() throws Exception {
        HttpRequestTemplate template = HttpRequestTemplate.builder("localhost", 9200).fromUrl("http:://127.0.0.1:12345").build();
        HttpInput httpInput = new HttpInput(template, HttpContentType.TEXT, null);
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, logger, httpClient, templateEngine);

        Map<String, String[]> headers = new HashMap<>(1);
        String contentType = randomFrom("application/json", "application/json; charset=UTF-8", "text/html", "application/yaml",
                "application/smile", "application/cbor");
        headers.put("Content-Type", new String[] { contentType });
        String body = "{\"foo\":\"bar\"}";
        HttpResponse httpResponse = new HttpResponse(200, body, headers);
        when(httpClient.execute(any())).thenReturn(httpResponse);

        HttpInput.Result result = input.execute(createWatchExecutionContext(), Payload.EMPTY);
        assertThat(result.payload().data(), hasEntry("_value", body));
        assertThat(result.payload().data(), not(hasKey("foo")));
    }

    public void testThatStatusCodeIsSetInResultAndPayload() throws Exception {
        HttpResponse response = new HttpResponse(200);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder("localhost", 8080);
        HttpInput httpInput = InputBuilders.httpInput(request.build()).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, logger, httpClient, templateEngine);

        WatchExecutionContext ctx = createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.statusCode, is(200));
        assertThat(result.payload().data(), hasKey("_status_code"));
        assertThat(result.payload().data().get("_status_code"), is(200));
    }

    private WatchExecutionContext createWatchExecutionContext() {
        Watch watch = new Watch("test-watch",
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                AlwaysCondition.INSTANCE,
                null,
                null,
                new ArrayList<>(),
                null,
                new WatchStatus(new DateTime(0, UTC), emptyMap()));
        return new TriggeredExecutionContext(watch,
                new DateTime(0, UTC),
                new ScheduleTriggerEvent(watch.id(), new DateTime(0, UTC), new DateTime(0, UTC)),
                TimeValue.timeValueSeconds(5));
    }
}
