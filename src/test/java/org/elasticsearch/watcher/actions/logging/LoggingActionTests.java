/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.actions.email.service.Attachment;
import org.elasticsearch.watcher.support.template.ValueTemplate;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

/**
 */
public class LoggingActionTests extends ElasticsearchTestCase {

    private ESLogger actionLogger;
    private LoggingLevel level;

    @Before
    public void init() throws IOException {
        actionLogger = mock(ESLogger.class);
        level = randomFrom(LoggingLevel.values());
    }

    @Test @Repeat(iterations = 30)
    public void testExecute() throws Exception {
        final DateTime now = DateTime.now(UTC);

        final Map<String, Object> expectedModel = ImmutableMap.<String, Object>builder()
                .put("ctx", ImmutableMap.builder()
                        .put("execution_time", now)
                        .put("watch_id", "_watch_name")
                        .put("payload", ImmutableMap.of())
                        .put("trigger", ImmutableMap.builder()
                                .put("scheduled_time", now)
                                .put("triggered_time", now)
                                .build())
                        .build())
                .build();

        String text = randomAsciiOfLength(10);
        LoggingAction action = new LoggingAction(logger, actionLogger, "_category", level, new ValueTemplate(text) {
            @Override
            public String render(Map<String, Object> model) {
                assertThat(model, equalTo((Object) expectedModel));
                return super.render(model);
            }
        });

        Watch watch = mock(Watch.class);
        when(watch.name()).thenReturn("_watch_name");
        WatchExecutionContext ctx = new WatchExecutionContext("_ctx_id", watch, now, new ScheduleTriggerEvent(now, now));

        LoggingAction.Result result = action.execute("_id", ctx, new Payload.Simple());
        verifyLogger(actionLogger, level, text);

        assertThat(result, notNullValue());
        assertThat(result.success(), is(true));
        assertThat(result, instanceOf(LoggingAction.Result.Success.class));
        assertThat(((LoggingAction.Result.Success) result).loggedText(), is(text));
    }

    @Test @Repeat(iterations = 10)
    public void testParser() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("text", new ValueTemplate(text));
        String category = null;
        if (randomBoolean()) {
            category = randomAsciiOfLength(10);
            builder.field("category", category);
        }
        LoggingLevel level = null;
        if (randomBoolean()) {
            level = randomFrom(LoggingLevel.values());
            builder.field("level", level);
        }
        builder.endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        LoggingAction action = parser.parse(xContentParser);

        assertThat(action, notNullValue());
        assertThat(action.category(), is(category));
        assertThat(action.level(), level == null ? is(LoggingLevel.INFO) : is(level));
        assertThat(action.logger(), notNullValue());
        assertThat(action.template(), notNullValue());
        assertThat(action.template().render(Collections.<String, Object>emptyMap()), is(text));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SelfGenerated() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);
        String category = randomAsciiOfLength(10);
        LoggingAction action = new LoggingAction(logger, actionLogger, category, level, new ValueTemplate(text));
        XContentBuilder builder = jsonBuilder();
        action.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        LoggingAction parsedAction = parser.parse(xContentParser);

        assertThat(parsedAction, equalTo(action));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SourceBuilder() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);
        LoggingAction.SourceBuilder sourceBuilder = loggingAction("_id", new ValueTemplate.SourceBuilder(text));
        String category = null;
        if (randomBoolean()) {
            category = randomAsciiOfLength(10);
            sourceBuilder.category(category);
        }
        LoggingLevel level = null;
        if (randomBoolean()) {
            level = randomFrom(LoggingLevel.values());
            sourceBuilder.level(level);
        }

        XContentBuilder builder = jsonBuilder();
        sourceBuilder.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());

        assertThat(xContentParser.nextToken(), is(XContentParser.Token.START_OBJECT));
        assertThat(xContentParser.nextToken(), is(XContentParser.Token.FIELD_NAME));
        assertThat(xContentParser.currentName(), is(LoggingAction.TYPE));
        assertThat(xContentParser.nextToken(), is(XContentParser.Token.START_OBJECT));
        LoggingAction action = parser.parse(xContentParser);
        assertThat(action, notNullValue());
        assertThat(action.category(), is(category));
        assertThat(action.level(), level == null ? is(LoggingLevel.INFO) : is(level));
        assertThat(action.logger(), notNullValue());
        assertThat(action.template(), notNullValue());
        assertThat(action.template(), Matchers.instanceOf(ValueTemplate.class));
        assertThat(action.template().render(Collections.<String, Object>emptyMap()), is(text));
    }

    @Test(expected = ActionException.class)
    public void testParser_Failure() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        XContentBuilder builder = jsonBuilder()
                .startObject().endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        parser.parse(xContentParser);
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result_Success() throws Exception {

        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", true)
                .field("logged_text", text)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        LoggingAction.Result result = parser.parseResult(xContentParser);
        assertThat(result, Matchers.notNullValue());
        assertThat(result.success(), is(true));
        assertThat(result, Matchers.instanceOf(LoggingAction.Result.Success.class));
        assertThat(((LoggingAction.Result.Success) result).loggedText(), is(text));
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result_Failure() throws Exception {

        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String reason = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", false)
                .field("reason", reason)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        LoggingAction.Result result = parser.parseResult(xContentParser);
        assertThat(result, Matchers.notNullValue());
        assertThat(result.success(), is(false));
        assertThat(result, Matchers.instanceOf(LoggingAction.Result.Failure.class));
        assertThat(((LoggingAction.Result.Failure) result).reason(), is(reason));
    }

    @Test(expected = ActionException.class)
    public void testParser_Result_MissingSuccessField() throws Exception {

        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("logged_text", text)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no success boolean field
        parser.parseResult(xContentParser);
    }

    @Test(expected = ActionException.class)
    public void testParser_Result_Failure_WithoutReason() throws Exception {

        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", false);
        if (randomBoolean()) {
            builder.field("logged_text", text);
        }
        builder.endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as the reason field is missing for the failure result
        parser.parseResult(xContentParser);
    }

    @Test(expected = ActionException.class)
    public void testParser_Result_Success_WithoutLoggedText() throws Exception {

        Settings settings = ImmutableSettings.EMPTY;
        ValueTemplate.Parser templateParser = new ValueTemplate.Parser();
        LoggingAction.Parser parser = new LoggingAction.Parser(settings, templateParser);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", true);
        if (randomBoolean()) {
            builder.field("reason", text);
        }
        builder.endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as the logged_text field is missing for the successful result
        parser.parseResult(xContentParser);
    }

    static void verifyLogger(ESLogger logger, LoggingLevel level, String text) {
        switch (level) {
            case ERROR:
                verify(logger, times(1)).error(text);
                break;
            case WARN:
                verify(logger, times(1)).warn(text);
                break;
            case INFO:
                verify(logger, times(1)).info(text);
                break;
            case DEBUG:
                verify(logger, times(1)).debug(text);
                break;
            case TRACE:
                verify(logger, times(1)).trace(text);
                break;
            default:
                fail("unhandled logging level [" + level.name() + "]");
        }
    }

}
