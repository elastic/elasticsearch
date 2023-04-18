/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.email.Attachment;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoggingActionTests extends ESTestCase {

    private Logger actionLogger;
    private LoggingLevel level;
    private TextTemplateEngine engine;

    @Before
    public void init() throws IOException {
        actionLogger = mock(Logger.class);
        level = randomFrom(LoggingLevel.values());
        engine = mock(TextTemplateEngine.class);
    }

    public void testExecute() throws Exception {
        final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContextBuilder("_watch_id").time("_watch_id", now).buildMock();

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

        String text = randomAlphaOfLength(10);
        TextTemplate template = new TextTemplate(text);
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
        LoggingActionFactory parser = new LoggingActionFactory(engine);

        String text = randomAlphaOfLength(10);
        TextTemplate template = new TextTemplate(text);

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("text", template);
        String category = null;
        if (randomBoolean()) {
            category = randomAlphaOfLength(10);
            builder.field("category", category);
        }
        LoggingLevel level = null;
        if (randomBoolean()) {
            level = randomFrom(LoggingLevel.values());
            builder.field("level", level);
        }
        builder.endObject();

        XContentParser xContentParser = createParser(builder);
        xContentParser.nextToken();

        ExecutableLoggingAction executable = parser.parseExecutable(randomAlphaOfLength(5), randomAlphaOfLength(3), xContentParser);

        assertThat(executable, notNullValue());
        assertThat(executable.action().category, is(category));
        assertThat(executable.action().level, level == null ? is(LoggingLevel.INFO) : is(level));
        assertThat(executable.textLogger(), notNullValue());
        assertThat(executable.action().text, notNullValue());
        assertThat(executable.action().text, is(template));
    }

    public void testParserSelfGenerated() throws Exception {
        LoggingActionFactory parser = new LoggingActionFactory(engine);

        String text = randomAlphaOfLength(10);
        TextTemplate template = new TextTemplate(text);
        String category = randomAlphaOfLength(10);
        LoggingAction action = new LoggingAction(template, level, category);
        ExecutableLoggingAction executable = new ExecutableLoggingAction(action, logger, engine);
        XContentBuilder builder = jsonBuilder();
        executable.toXContent(builder, Attachment.XContent.EMPTY_PARAMS);

        XContentParser xContentParser = createParser(builder);
        xContentParser.nextToken();

        ExecutableLoggingAction parsedAction = parser.parseExecutable(randomAlphaOfLength(5), randomAlphaOfLength(5), xContentParser);

        assertThat(parsedAction, equalTo(executable));
    }

    public void testParserBuilder() throws Exception {
        LoggingActionFactory parser = new LoggingActionFactory(engine);

        String text = randomAlphaOfLength(10);
        TextTemplate template = new TextTemplate(text);
        LoggingAction.Builder actionBuilder = loggingAction(template);
        if (randomBoolean()) {
            actionBuilder.setCategory(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            actionBuilder.setLevel(randomFrom(LoggingLevel.values()));
        }
        LoggingAction action = actionBuilder.build();

        XContentBuilder builder = jsonBuilder().value(action);
        XContentParser xContentParser = createParser(builder);

        assertThat(xContentParser.nextToken(), is(XContentParser.Token.START_OBJECT));
        ExecutableLoggingAction executable = parser.parseExecutable(randomAlphaOfLength(4), randomAlphaOfLength(5), xContentParser);
        assertThat(executable, notNullValue());
        assertThat(executable.action(), is(action));
        assertThat(executable.action(), is(action));
        assertThat(executable.action(), is(action));
    }

    public void testParserFailure() throws Exception {
        LoggingActionFactory parser = new LoggingActionFactory(engine);

        XContentBuilder builder = jsonBuilder().startObject().endObject();

        XContentParser xContentParser = createParser(builder);
        xContentParser.nextToken();

        try {
            parser.parseExecutable(randomAlphaOfLength(5), randomAlphaOfLength(5), xContentParser);
            fail("Expected failure as there's no text");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("missing required [text] field"));
        }
    }

    @SuppressLoggerChecks(reason = "mock usage")
    static void verifyLogger(Logger logger, LoggingLevel level, String text) {
        switch (level) {
            case ERROR -> verify(logger, times(1)).error(text);
            case WARN -> verify(logger, times(1)).warn(text);
            case INFO -> verify(logger, times(1)).info(text);
            case DEBUG -> verify(logger, times(1)).debug(text);
            case TRACE -> verify(logger, times(1)).trace(text);
            default -> fail("unhandled logging level [" + level.name() + "]");
        }
    }

}
