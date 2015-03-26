/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.http;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Actions;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.support.http.*;
import org.elasticsearch.watcher.support.http.auth.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class HttpInputTests extends ElasticsearchTestCase {

    private HttpClient httpClient;
    private HttpInput.Parser httpParser;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
        Template.Parser templateParser = new MockTemplate.Parser();
        HttpAuthRegistry registry = new HttpAuthRegistry(ImmutableMap.of("basic", new BasicAuth.Parser()));
        httpParser = new HttpInput.Parser(
                ImmutableSettings.EMPTY, httpClient, new HttpRequest.Parser(registry), new TemplatedHttpRequest.Parser(templateParser, registry)
        );
    }

    @Test
    public void testExecute() throws Exception {
        String host = "_host";
        int port = 123;
        String body = "_body";
        TemplatedHttpRequest request = new TemplatedHttpRequest();
        request.method(HttpMethod.POST);
        request.host(host);
        request.port(port);
        Template mockBody = mock(Template.class);
        when(mockBody.render(anyMap())).thenReturn(body);
        request.body(mockBody);
        HttpInput input = new HttpInput(logger, httpClient, request);

        HttpResponse response = new HttpResponse();
        response.status(123);
        response.inputStream(new ByteArrayInputStream("{\"key\" : \"value\"}".getBytes(UTF8)));
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        Watch watch = new Watch("test-watch",
                new ClockMock(),
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                new SimpleInput(logger, new Payload.Simple()),
                new AlwaysTrueCondition(logger),
                null,
                new Actions(new ArrayList<Action>()),
                null,
                null,
                new Watch.Status());
        WatchExecutionContext ctx = new WatchExecutionContext("test-watch1",
                watch,
                new DateTime(0, DateTimeZone.UTC),
                new ScheduleTriggerEvent(new DateTime(0, DateTimeZone.UTC), new DateTime(0, DateTimeZone.UTC)));
        HttpInput.Result result = input.execute(ctx);
        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.payload().data(), equalTo(MapBuilder.<String, Object>newMapBuilder().put("key", "value").map()));
    }

    @Test
    @Repeat(iterations = 12)
    public void testParser() throws Exception {
        final String httpMethod = randomFrom("PUT", "POST", "GET", "DELETE", "HEAD", null);
        String host = randomAsciiOfLength(3);
        int port = randomInt();
        Template path = new MockTemplate(randomAsciiOfLength(3));
        String body = randomBoolean() ? randomAsciiOfLength(3) : null;
        Map<String, Template> params = randomBoolean() ? new MapBuilder<String, Template>().put("a", new MockTemplate("b")).map() : null;
        Map<String, Template> headers = randomBoolean() ? new MapBuilder<String, Template>().put("c", new MockTemplate("d")).map() : null;
        HttpAuth auth = randomBoolean() ? new BasicAuth("username", "password") : null;
        HttpInput.SourceBuilder sourceBuilder = new HttpInput.SourceBuilder()
                .setMethod(httpMethod)
                .setHost(host)
                .setPort(port)
                .setPath(path)
                .setBody(body != null ? new MockTemplate(body) : null)
                .setParams(params)
                .setHeaders(headers)
                .setAuth(auth);
        XContentParser parser = XContentHelper.createParser(jsonBuilder().value(sourceBuilder).bytes());
        parser.nextToken();
        HttpInput result = httpParser.parse(parser);

        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.getRequest().method().method(), equalTo(httpMethod != null ? httpMethod : "GET")); // get is the default
        assertThat(result.getRequest().host(), equalTo(host));
        assertThat(result.getRequest().port(), equalTo(port));
        assertThat(result.getRequest().path(), equalTo(path));
        assertThat(result.getRequest().params(), equalTo(params));
        assertThat(result.getRequest().headers(), equalTo(headers));
        assertThat(result.getRequest().auth(), equalTo(auth));
        if (body != null) {
            assertThat(result.getRequest().body().render(Collections.<String, Object>emptyMap()), equalTo(body));
        } else {
            assertThat(result.getRequest().body(), nullValue());
        }
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testParser_invalidHttpMethod() throws Exception {
        Map<String, Template> headers = new MapBuilder<String, Template>().put("a", new MockTemplate("b")).map();
        HttpInput.SourceBuilder sourceBuilder = new HttpInput.SourceBuilder()
                .setMethod("_method")
                .setHost("_host")
                .setPort(123)
                .setBody(new MockTemplate("_body"))
                .setHeaders(headers);
        XContentParser parser = XContentHelper.createParser(jsonBuilder().value(sourceBuilder).bytes());
        parser.nextToken();
        httpParser.parse(parser);
    }

    @Test
    public void testParseResult() throws Exception {
        String httpMethod = "get";
        String body = "_body";
        Map<String, Template> headers = new MapBuilder<String, Template>().put("a", new MockTemplate("b")).map();
        HttpInput.SourceBuilder sourceBuilder = new HttpInput.SourceBuilder()
                .setMethod(httpMethod)
                .setHost("_host")
                .setPort(123)
                .setBody(new MockTemplate(body))
                .setHeaders(headers);

        Map<String, Object> payload = MapBuilder.<String, Object>newMapBuilder().put("x", "y").map();

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field(HttpInput.Parser.HTTP_STATUS_FIELD.getPreferredName(), 123);
        builder.field(HttpInput.Parser.REQUEST_FIELD.getPreferredName(), sourceBuilder);
        builder.field(Input.Result.PAYLOAD_FIELD.getPreferredName(), payload);
        builder.endObject();

        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();
        HttpInput.Result result = httpParser.parseResult(parser);
        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.payload().data(), equalTo(payload));
        assertThat(result.statusCode(), equalTo(123));
        assertThat(result.request().method().method(), equalTo("GET"));
        assertThat(result.request().headers().size(), equalTo(headers.size()));
        for (Map.Entry<String, Template> entry : headers.entrySet()) {
            assertThat(entry.getValue().render(Collections.<String, Object>emptyMap()), equalTo(result.request().headers().get(entry.getKey())));
        }
        assertThat(result.request().host(), equalTo("_host"));
        assertThat(result.request().port(), equalTo(123));
        assertThat(result.request().body(), equalTo("_body"));
    }

    private static class MockTemplate implements Template {

        private final String value;

        private MockTemplate(String value) {
            this.value = value;
        }

        @Override
        public String render(Map<String, Object> model) {
            return value;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockTemplate that = (MockTemplate) o;

            if (!value.equals(that.value)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        static class Parser implements Template.Parser {

            @Override
            public Template parse(XContentParser parser) throws IOException, ParseException {
                String value = parser.text();
                return new MockTemplate(value);
            }
        }

    }

}
