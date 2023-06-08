/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.http;

import io.netty.handler.codec.http.HttpHeaders;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpContentType;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.input.InputBuilders;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
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
        httpParser = new HttpInputFactory(Settings.EMPTY, httpClient, templateEngine);
    }

    public void testExecute() throws Exception {
        String host = "_host";
        int port = 123;
        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder(host, port).method(HttpMethod.POST).body("_body");
        HttpInput httpInput;

        HttpResponse response;
        switch (randomIntBetween(1, 6)) {
            case 1 -> {
                response = new HttpResponse(123, "{\"key\" : \"value\"}".getBytes(StandardCharsets.UTF_8));
                httpInput = InputBuilders.httpInput(request.build()).build();
            }
            case 2 -> {
                response = new HttpResponse(123, "---\nkey : value".getBytes(StandardCharsets.UTF_8));
                httpInput = InputBuilders.httpInput(request.build()).expectedResponseXContentType(HttpContentType.YAML).build();
            }
            case 3 -> {
                response = new HttpResponse(
                    123,
                    "{\"key\" : \"value\"}".getBytes(StandardCharsets.UTF_8),
                    singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { XContentType.JSON.mediaType() })
                );
                httpInput = InputBuilders.httpInput(request.build()).build();
            }
            case 4 -> {
                response = new HttpResponse(
                    123,
                    "key: value".getBytes(StandardCharsets.UTF_8),
                    singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { XContentType.YAML.mediaType() })
                );
                httpInput = InputBuilders.httpInput(request.build()).build();
            }
            case 5 -> {
                response = new HttpResponse(
                    123,
                    "---\nkey: value".getBytes(StandardCharsets.UTF_8),
                    singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { "unrecognized_content_type" })
                );
                httpInput = InputBuilders.httpInput(request.build()).expectedResponseXContentType(HttpContentType.YAML).build();
            }
            default -> {
                response = new HttpResponse(
                    123,
                    "{\"key\" : \"value\"}".getBytes(StandardCharsets.UTF_8),
                    singletonMap(HttpHeaders.Names.CONTENT_TYPE, new String[] { "unrecognized_content_type" })
                );
                httpInput = InputBuilders.httpInput(request.build()).build();
            }
        }

        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);
        when(templateEngine.render(eq(new TextTemplate("_body")), anyMap())).thenReturn("_body");

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.payload().data(), hasEntry("key", "value"));
    }

    public void testExecuteNonJson() throws Exception {
        String host = "_host";
        int port = 123;
        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder(host, port).method(HttpMethod.POST).body("_body");
        HttpInput httpInput = InputBuilders.httpInput(request.build()).expectedResponseXContentType(HttpContentType.TEXT).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);
        String notJson = "This is not json";
        HttpResponse response = new HttpResponse(123, notJson.getBytes(StandardCharsets.UTF_8));
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);
        when(templateEngine.render(eq(new TextTemplate("_body")), anyMap())).thenReturn("_body");

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.payload().data().get("_value").toString(), equalTo(notJson));
    }

    public void testParser() throws Exception {
        final HttpMethod httpMethod = rarely() ? null : randomFrom(HttpMethod.values());
        Scheme scheme = randomFrom(Scheme.HTTP, Scheme.HTTPS, null);
        String host = randomAlphaOfLength(3);
        int port = randomIntBetween(8000, 9000);
        String path = randomAlphaOfLength(3);
        TextTemplate pathTemplate = new TextTemplate(path);
        String body = randomBoolean() ? randomAlphaOfLength(3) : null;
        Map<String, TextTemplate> params = randomBoolean() ? Map.of("a", new TextTemplate("b")) : null;
        Map<String, TextTemplate> headers = randomBoolean() ? Map.of("c", new TextTemplate("d")) : null;
        BasicAuth auth = randomBoolean() ? new BasicAuth("username", "password".toCharArray()) : null;
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

        String[] extractKeys = randomFrom(new String[] { "foo", "bar" }, new String[] { "baz" }, null);
        if (expectedResponseXContentType != HttpContentType.TEXT) {
            if (extractKeys != null) {
                inputBuilder.extractKeys(extractKeys);
            }
        }

        inputBuilder.expectedResponseXContentType(expectedResponseXContentType);
        XContentBuilder source = jsonBuilder();
        inputBuilder.build().toXContent(source, WatcherParams.builder().hideSecrets(false).build());
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> httpParser.parseInput("_id", parser));
        assertThat(e.getMessage(), is("unsupported http method [_METHOD]"));
    }

    public void testThatHeadersAreIncludedInPayload() throws Exception {
        String headerName = randomAlphaOfLength(10);
        String headerValue = randomAlphaOfLength(10);
        boolean responseHasContent = randomBoolean();

        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder("localhost", 8080);
        HttpInput httpInput = InputBuilders.httpInput(request.build()).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);

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

        when(templateEngine.render(eq(new TextTemplate("_body")), anyMap())).thenReturn("_body");

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
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
        HttpRequestTemplate template = HttpRequestTemplate.builder("http:://127.0.0.1:12345").build();
        HttpInput httpInput = new HttpInput(template, HttpContentType.TEXT, null);
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);

        Map<String, String[]> headers = Maps.newMapWithExpectedSize(1);
        String contentType = randomFrom(
            "application/json",
            "application/json;charset=utf-8",
            "text/html",
            "application/yaml",
            "application/smile",
            "application/cbor"
        );
        headers.put("Content-Type", new String[] { contentType });
        String body = "{\"foo\":\"bar\"}";
        HttpResponse httpResponse = new HttpResponse(200, body, headers);
        when(httpClient.execute(any())).thenReturn(httpResponse);

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, Payload.EMPTY);
        assertThat(result.payload().data(), hasEntry("_value", body));
        assertThat(result.payload().data(), not(hasKey("foo")));
    }

    public void testThatStatusCodeIsSetInResultAndPayload() throws Exception {
        HttpResponse response = new HttpResponse(200);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder("localhost", 8080);
        HttpInput httpInput = InputBuilders.httpInput(request.build()).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.statusCode, is(200));
        assertThat(result.payload().data(), hasKey("_status_code"));
        assertThat(result.payload().data().get("_status_code"), is(200));
    }

    @SuppressWarnings("unchecked")
    public void testThatArrayJsonResponseIsHandled() throws Exception {
        Map<String, String[]> headers = Collections.singletonMap("Content-Type", new String[] { "application/json" });
        HttpResponse response = new HttpResponse(200, "[ { \"foo\":  \"first\" }, {  \"foo\":  \"second\"}]", headers);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder("localhost", 8080);
        HttpInput httpInput = InputBuilders.httpInput(request.build()).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.statusCode, is(200));
        assertThat(result.payload().data(), not(hasKey("_value")));
        assertThat(result.payload().data(), hasKey("data"));
        assertThat(result.payload().data().get("data"), instanceOf(List.class));
        List<Map<String, String>> data = (List<Map<String, String>>) result.payload().data().get("data");
        assertThat(data, hasSize(2));
        assertThat(data.get(0).get("foo"), is("first"));
        assertThat(data.get(1).get("foo"), is("second"));
    }

    public void testExceptionCase() throws Exception {
        when(httpClient.execute(any(HttpRequest.class))).thenThrow(new IOException("could not connect"));

        HttpRequestTemplate.Builder request = HttpRequestTemplate.builder("localhost", 8080);
        HttpInput httpInput = InputBuilders.httpInput(request.build()).build();
        ExecutableHttpInput input = new ExecutableHttpInput(httpInput, httpClient, templateEngine);

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        HttpInput.Result result = input.execute(ctx, new Payload.Simple());

        assertThat(result.getException(), is(notNullValue()));
        assertThat(result.getException(), is(instanceOf(IOException.class)));
        assertThat(result.getException().getMessage(), is("could not connect"));

        try (XContentBuilder builder = jsonBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference bytes = BytesReference.bytes(builder);
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, bytes.streamInput())
            ) {
                Map<String, Object> data = parser.map();
                String reason = ObjectPath.eval("error.reason", data);
                assertThat(reason, is("could not connect"));
                String type = ObjectPath.eval("error.type", data);
                assertThat(type, is("i_o_exception"));
            }
        }
    }
}
