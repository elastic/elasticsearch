/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.actions.email.service.Attachment;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.watch.Payload;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.joda.time.DateTimeZone.UTC;
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

    @Test
    public void testExecute() throws Exception {
        final DateTime now = DateTime.now(UTC);

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContextBuilder("_watch_id")
                .time("_watch_id", now)
                .buildMock();

        final Map<String, Object> expectedModel = ImmutableMap.<String, Object>builder()
                .put("ctx", ImmutableMap.builder()
                        .put("id", ctx.id().value())
                        .put("watch_id", "_watch_id")
                        .put("execution_time", now)
                        .put("payload", ImmutableMap.of())
                        .put("metadata", ImmutableMap.of())
                        .put("trigger", ImmutableMap.builder()
                                .put("scheduled_time", now)
                                .put("triggered_time", now)
                                .build())
                        .build())
                .build();

        String text = randomAsciiOfLength(10);
        Template template = Template.inline(text).build();
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

    @Test
    public void testParser() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        Template template = Template.inline(text).build();

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

    @Test
    public void testParser_SelfGenerated() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        Template template = Template.inline(text).build();
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

    @Test
    public void testParser_Builder() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        String text = randomAsciiOfLength(10);
        Template template = Template.inline(text).build();
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

    @Test(expected = ActionException.class)
    public void testParser_Failure() throws Exception {
        Settings settings = Settings.EMPTY;
        LoggingActionFactory parser = new LoggingActionFactory(settings, engine);

        XContentBuilder builder = jsonBuilder()
                .startObject().endObject();

        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(builder.bytes());
        xContentParser.nextToken();

        // will fail as there's no text
        parser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(5), xContentParser);
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
