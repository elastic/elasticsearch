/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.actions.email.service.Attachment;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.watch.Payload;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
    private TemplateEngine engine;

    @Before
    public void init() throws IOException {
        actionLogger = mock(ESLogger.class);
        level = randomFrom(LoggingLevel.values());
        engine = mock(TemplateEngine.class);
    }

    @Test @Repeat(iterations = 30)
    public void testExecute() throws Exception {
        final DateTime now = DateTime.now(UTC);

        final Map<String, Object> expectedModel = ImmutableMap.<String, Object>builder()
                .put("ctx", ImmutableMap.builder()
                        .put("execution_time", now)
                        .put("watch_id", "_watch_id")
                        .put("payload", ImmutableMap.of())
                        .put("metadata", ImmutableMap.of())
                        .put("trigger", ImmutableMap.builder()
                                .put("scheduled_time", now)
                                .put("triggered_time", now)
                                .build())
                        .build())
                .build();

        String text = randomAsciiOfLength(10);
        Template template = new Template(text);
        LoggingAction action = new LoggingAction(template, level, "_category");
        ExecutableLoggingAction executable = new ExecutableLoggingAction(action, logger, actionLogger, engine);
        when(engine.render(template, expectedModel)).thenReturn(text);

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContextBuilder("_watch_id")
                .time("_watch_id", now)
                .buildMock();

        LoggingAction.Result result = executable.execute("_id", ctx, new Payload.Simple());
        verifyLogger(actionLogger, level, text);

        assertThat(result, notNullValue());
        assertThat(result.success(), is(true));
        assertThat(result, instanceOf(LoggingAction.Result.Success.class));
        assertThat(((LoggingAction.Result.Success) result).loggedText(), is(text));
    }

    @Test @Repeat(iterations = 10)
    public void testParser() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        Template template = new Template(text);

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("text", template);
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

        ExecutableLoggingAction executable = parser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(3), xContentParser);

        assertThat(executable, notNullValue());
        assertThat(executable.action().category, is(category));
        assertThat(executable.action().level, level == null ? is(LoggingLevel.INFO) : is(level));
        assertThat(executable.textLogger(), notNullValue());
        assertThat(executable.action().text, notNullValue());
        assertThat(executable.action().text, is(template));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_SelfGenerated() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        Template template = new Template(text);
        String category = randomAsciiOfLength(10);
        LoggingAction action = new LoggingAction(template, level, category);
        ExecutableLoggingAction executable = new ExecutableLoggingAction(action, logger, settings, engine);
        XContentBuilder builder = jsonBuilder();
        executable.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        ExecutableLoggingAction parsedAction = parser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(5), xContentParser);

        assertThat(parsedAction, equalTo(executable));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_Builder() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        Template template = new Template(text);
        LoggingAction.Builder actionBuilder = loggingAction(template);
        String category = null;
        if (randomBoolean()) {
            category = randomAsciiOfLength(10);
            actionBuilder.setCategory(category);
        }
        LoggingLevel level = null;
        if (randomBoolean()) {
            level = randomFrom(LoggingLevel.values());
            actionBuilder.setLevel(level);
        }
        LoggingAction action = actionBuilder.build();

        XContentBuilder builder = jsonBuilder().value(action);
        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());

        assertThat(xContentParser.nextToken(), is(XContentParser.Token.START_OBJECT));
        ExecutableLoggingAction executable = parser.parseExecutable(randomAsciiOfLength(4), randomAsciiOfLength(5), xContentParser);
        assertThat(executable, notNullValue());
        assertThat(executable.action(), is(action));
    }

    @Test(expected = ActionException.class)
    public void testParser_Failure() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        XContentBuilder builder = jsonBuilder()
                .startObject().endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        parser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(5), xContentParser);
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result_Success() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", true)
                .field("logged_text", text)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        LoggingAction.Result result = parser.parseResult(wid, actionId, xContentParser);
        assertThat(result, Matchers.notNullValue());
        assertThat(result.success(), is(true));
        assertThat(result, Matchers.instanceOf(LoggingAction.Result.Success.class));
        assertThat(((LoggingAction.Result.Success) result).loggedText(), is(text));
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result_Failure() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        String reason = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", false)
                .field("reason", reason)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        LoggingAction.Result result = parser.parseResult(wid, actionId, xContentParser);
        assertThat(result, Matchers.notNullValue());
        assertThat(result.success(), is(false));
        assertThat(result, Matchers.instanceOf(LoggingAction.Result.Failure.class));
        assertThat(((LoggingAction.Result.Failure) result).reason(), is(reason));
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result_Simulated() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", true)
                .field("simulated_logged_text", text)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        LoggingAction.Result result = parser.parseResult(wid, actionId, xContentParser);
        assertThat(result, Matchers.notNullValue());
        assertThat(result.success(), is(true));
        assertThat(result, Matchers.instanceOf(LoggingAction.Result.Simulated.class));
        assertThat(((LoggingAction.Result.Simulated) result).loggedText(), is(text));
    }

    @Test
    public void testParser_Result_Simulated_SelfGenerated() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory actionParser = new LoggingActionFactory(settings, engine);
        String text = randomAsciiOfLength(10);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        LoggingAction.Result.Simulated simulatedResult = new LoggingAction.Result.Simulated(text);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        simulatedResult.toXContent(builder, ToXContent.EMPTY_PARAMS);

        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        LoggingAction.Result result = actionParser.parseResult(wid, actionId, xContentParser);
        assertThat(result, Matchers.notNullValue());
        assertThat(result.success(), is(true));
        assertThat(result, Matchers.instanceOf(LoggingAction.Result.Simulated.class));
        assertThat(((LoggingAction.Result.Simulated) result).loggedText(), is(text));
    }

    @Test(expected = ActionException.class)
    public void testParser_Result_MissingSuccessField() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        String text = randomAsciiOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject()
                .field("logged_text", text)
                .endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no success boolean field
        parser.parseResult(wid, actionId, xContentParser);
    }

    @Test(expected = ActionException.class)
    public void testParser_Result_Failure_WithoutReason() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

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
        parser.parseResult(wid, actionId, xContentParser);
    }

    @Test(expected = ActionException.class)
    public void testParser_Result_Success_WithoutLoggedText() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        Wid wid = new Wid(randomAsciiOfLength(3), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

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
        parser.parseResult(wid, actionId, xContentParser);
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
