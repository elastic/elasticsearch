/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.xpack.notification.email.Attachment;
import org.joda.time.DateTime;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 */
public class LoggingActionTests extends ESTestCase {
    private ESLogger actionLogger;
    private LoggingLevel level;
    private TextTemplateEngine engine;

    @Before
    public void init() throws IOException {
        actionLogger = mock(ESLogger.class);
        level = randomFrom(LoggingLevel.values());
        engine = mock(TextTemplateEngine.class);
    }

    public void testExecute() throws Exception {
        final DateTime now = DateTime.now(UTC);

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContextBuilder("_watch_id")
                .time("_watch_id", now)
                .buildMock();

        Map<String, Object> triggerModel = new HashMap<>();
        triggerModel.put("scheduled_time", now);
        triggerModel.put("triggered_time", now);
        Map<String, Object> ctxModel = new HashMap<>();
        ctxModel.put("id", ctx.id().value());
        ctxModel.put("watch_id", "_watch_id");
        ctxModel.put("execution_time", now);
        ctxModel.put("payload", emptyMap());
        ctxModel.put("metadata", emptyMap());
        ctxModel.put("vars", emptyMap());
        ctxModel.put("trigger", triggerModel);
        Map<String, Object> expectedModel = singletonMap("ctx", ctxModel);

        String text = randomAsciiOfLength(10);
        TextTemplate template = TextTemplate.inline(text).build();
        LoggingAction action = new LoggingAction(template, level, "_category");
        ExecutableLoggingAction executable = new ExecutableLoggingAction(action, logger, actionLogger, engine);
        when(engine.render(template, expectedModel)).thenReturn(text);



        Action.Result result = executable.execute("_id", ctx, new Payload.Simple());
        verifyLogger(actionLogger, level, text);

        assertThat(result, notNullValue());
        assertThat(result.status(), is(Action.Result.Status.SUCCESS));
        assertThat(result, instanceOf(LoggingAction.Result.Success.class));
        assertThat(((LoggingAction.Result.Success) result).loggedText(), is(text));
    }

    public void testParser() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        TextTemplate template = TextTemplate.inline(text).build();

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

    public void testParserSelfGenerated() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        TextTemplate template = TextTemplate.inline(text).build();
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

    public void testParserBuilder() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        TextTemplate template = TextTemplate.inline(text).build();
        LoggingAction.Builder actionBuilder = loggingAction(template);
        if (randomBoolean()) {
            actionBuilder.setCategory(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            actionBuilder.setLevel(randomFrom(LoggingLevel.values()));
        }
        LoggingAction action = actionBuilder.build();

        XContentBuilder builder = jsonBuilder().value(action);
        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());

        assertThat(xContentParser.nextToken(), is(XContentParser.Token.START_OBJECT));
        ExecutableLoggingAction executable = parser.parseExecutable(randomAsciiOfLength(4), randomAsciiOfLength(5), xContentParser);
        assertThat(executable, notNullValue());
        assertThat(executable.action(), is(action));
        assertThat(executable.action(), is(action));
        assertThat(executable.action(), is(action));
    }

    public void testParserFailure() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        XContentBuilder builder = jsonBuilder()
                .startObject().endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        try {
            parser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(5), xContentParser);
            fail("Expected failure as there's no text");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("missing required [text] field"));
        }
    }

    @SuppressLoggerChecks(reason = "mock usage")
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
