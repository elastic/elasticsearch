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
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.Actions;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.InputBuilders;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.support.http.*;
import org.elasticsearch.watcher.support.http.auth.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.ValueTemplate;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;
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
        Template.Parser templateParser = new ValueTemplate.Parser();
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
        HttpInput input = new HttpInput(logger, httpClient, request, null);

        HttpResponse response = new HttpResponse(123, "{\"key\" : \"value\"}".getBytes(UTF8));
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        Watch watch = new Watch("test-watch",
                new ClockMock(),
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                new SimpleInput(logger, new Payload.Simple()),
                new AlwaysTrueCondition(logger),
                null,
                new Actions(new ArrayList<ActionWrapper>()),
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

    @Test @Repeat(iterations = 20)
    public void testParser() throws Exception {
        final HttpMethod httpMethod = rarely() ? null : randomFrom(HttpMethod.values());
        String scheme = randomFrom("http", "https", null);
        String host = randomAsciiOfLength(3);
        int port = randomIntBetween(8000, 9000);
        String path = randomAsciiOfLength(3);
        Template.SourceBuilder pathTemplate = mockTemplateSourceBuilder(path);
        String body = randomBoolean() ? randomAsciiOfLength(3) : null;
        Map<String, Template.SourceBuilder> params = randomBoolean() ? new MapBuilder<String, Template.SourceBuilder>().put("a", mockTemplateSourceBuilder("b")).map() : null;
        Map<String, Template.SourceBuilder> headers = randomBoolean() ? new MapBuilder<String, Template.SourceBuilder>().put("c", mockTemplateSourceBuilder("d")).map() : null;
        HttpAuth auth = randomBoolean() ? new BasicAuth("username", "password") : null;
        TemplatedHttpRequest.SourceBuilder requestSource = new TemplatedHttpRequest.SourceBuilder(host, port)
                .setScheme(scheme)
                .setMethod(httpMethod)
                .setPath(pathTemplate)
                .setBody(body != null ? new ValueTemplate(body) : null)
                .setAuth(auth);

        if (params != null) {
            requestSource.putParams(params);
        }
        if (headers != null) {
            requestSource.putHeaders(headers);
        }

        XContentParser parser = XContentHelper.createParser(jsonBuilder().value(InputBuilders.httpInput(requestSource)).bytes());
        parser.nextToken();
        HttpInput result = httpParser.parse(parser);

        assertThat(result.type(), equalTo(HttpInput.TYPE));
        assertThat(result.getRequest().scheme().scheme(), equalTo(scheme != null ? scheme : "http")); // http is the default
        assertThat(result.getRequest().method(), equalTo(httpMethod != null ? httpMethod : HttpMethod.GET)); // get is the default
        assertThat(result.getRequest().host(), equalTo(host));
        assertThat(result.getRequest().port(), equalTo(port));
        assertThat(result.getRequest().path(), isTemplate(path));
        if (params != null) {
            assertThat(result.getRequest().params(), hasEntry(is("a"), isTemplate("b")));
        }
        if (headers != null) {
            assertThat(result.getRequest().headers(), hasEntry(is("c"), isTemplate("d")));
        }
        assertThat(result.getRequest().auth(), equalTo(auth));
        if (body != null) {
            assertThat(result.getRequest().body().render(Collections.<String, Object>emptyMap()), equalTo(body));
        } else {
            assertThat(result.getRequest().body(), nullValue());
        }
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testParser_invalidHttpMethod() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .startObject("request")
                    .field("method", "_method")
                    .field("body", "_body")
                .endObject()
                .endObject();
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        parser.nextToken();
        httpParser.parse(parser);
    }

    @Test
    public void testParseResult() throws Exception {
        String httpMethod = "get";
        String body = "_body";
        Map<String, Template.SourceBuilder> headers = new MapBuilder<String, Template.SourceBuilder>().put("a", mockTemplateSourceBuilder("b")).map();
        HttpRequest.SourceBuilder sourceBuilder = new HttpRequest.SourceBuilder()
                .setMethod(httpMethod)
                .setHost("_host")
                .setPort(123)
                .setBody(new ValueTemplate(body))
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
        assertThat(result.request().headers(), hasEntry("a", (Object) "b"));
        assertThat(result.request().host(), equalTo("_host"));
        assertThat(result.request().port(), equalTo(123));
        assertThat(result.request().body(), equalTo("_body"));
    }

    private static Template mockTemplate(String value) {
        return new ValueTemplate(value);
    }

    private static Template.SourceBuilder mockTemplateSourceBuilder(String value) {
        return new Template.InstanceSourceBuilder(new ValueTemplate(value));
    }

    static MockTemplateMatcher isTemplate(String value) {
        return new MockTemplateMatcher(value);
    }

    static class MockTemplateMatcher extends BaseMatcher<Template> {

        private final String value;

        public MockTemplateMatcher(String value) {
            this.value = value;
        }

        @Override
        public boolean matches(Object item) {
            return item instanceof ValueTemplate && ((ValueTemplate) item).value().equals(value);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("is mock template [" + value + "]");
        }
    }

}
