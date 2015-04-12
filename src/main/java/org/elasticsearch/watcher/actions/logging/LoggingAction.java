/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionException;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class LoggingAction extends Action<LoggingAction.Result> {

    public static final String TYPE = "logging";

    private final Template text;
    private final @Nullable String category;
    private final @Nullable LoggingLevel level;

    private final ESLogger actionLogger;
    private final TemplateEngine templateEngine;

    public LoggingAction(ESLogger logger, Template text, @Nullable String category, @Nullable LoggingLevel level, Settings settings, TemplateEngine templateEngine) {
        super(logger);
        this.text = text;
        this.category = category;
        this.level = level != null ? level : LoggingLevel.INFO;
        this.actionLogger = category != null ? Loggers.getLogger(category, settings) : logger;
        this.templateEngine = templateEngine;
    }

    // for tests
    LoggingAction(ESLogger logger, Template text, @Nullable String category, @Nullable LoggingLevel level, ESLogger actionLogger, TemplateEngine templateEngine) {
        super(logger);
        this.text = text;
        this.category = category;
        this.level = level != null ? level : LoggingLevel.INFO;
        this.actionLogger = actionLogger;
        this.templateEngine = templateEngine;
    }

    @Override
    public String type() {
        return TYPE;
    }

    Template text() {
        return text;
    }

    String category() {
        return category;
    }

    LoggingLevel level() {
        return level;
    }

    ESLogger logger() {
        return actionLogger;
    }

    @Override
    protected Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        try {
            return doExecute(actionId, ctx, payload);
        } catch (Exception e) {
            return new Result.Failure("failed to execute [logging] action. error: " + e.getMessage());
        }
    }

    protected Result doExecute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        String loggedText = templateEngine.render(text, model);
        if (ctx.simulateAction(actionId)) {
            return new Result.Simulated(loggedText);
        }

        level.log(actionLogger, loggedText);
        return new Result.Success(loggedText);

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (category != null) {
            builder.field(Parser.CATEGORY_FIELD.getPreferredName(), category);
        }
        builder.field(Parser.LEVEL_FIELD.getPreferredName(), level);
        builder.field(Parser.TEXT_FIELD.getPreferredName(), text);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoggingAction action = (LoggingAction) o;
        return Objects.equals(text, action.text) &&
                Objects.equals(category, action.category) &&
                Objects.equals(level, action.level);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, category, level);
    }

    public static abstract class Result extends Action.Result {

        protected Result(boolean success) {
            super(TYPE, success);
        }

        public static class Success extends Result {

            private final String loggedText;

            public Success(String loggedText) {
                super(true);
                this.loggedText = loggedText;
            }

            public String loggedText() {
                return loggedText;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.LOGGED_TEXT_FIELD.getPreferredName(), loggedText);
            }
        }

        public static class Failure extends Result {

            private final String reason;

            public Failure(String reason) {
                super(false);
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.REASON_FIELD.getPreferredName(), reason);
            }
        }

        public static class Simulated extends Result {

            private final String loggedText;

            protected Simulated(String loggedText) {
                super(true);
                this.loggedText = loggedText;
            }

            public String loggedText() {
                return loggedText;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.SIMULATED_LOGGED_TEXT_FIELD.getPreferredName(), loggedText);
            }
        }
    }

    public static class Parser implements Action.Parser<LoggingAction.Result, LoggingAction> {

        static final ParseField CATEGORY_FIELD = new ParseField("category");
        static final ParseField LEVEL_FIELD = new ParseField("level");
        static final ParseField TEXT_FIELD = new ParseField("text");
        static final ParseField LOGGED_TEXT_FIELD = new ParseField("logged_text");
        static final ParseField SIMULATED_LOGGED_TEXT_FIELD = new ParseField("simulated_logged_text");
        static final ParseField REASON_FIELD = new ParseField("reason");

        private final Settings settings;
        private final TemplateEngine templateEngine;
        private final ESLogger logger;

        @Inject
        public Parser(Settings settings, TemplateEngine templateEngine) {
            this.settings = settings;
            this.logger = Loggers.getLogger(LoggingAction.class, settings);
            this.templateEngine = templateEngine;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public LoggingAction parse(XContentParser parser) throws IOException {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT;

            String category = null;
            LoggingLevel level = LoggingLevel.INFO;
            Template text = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (TEXT_FIELD.match(currentFieldName)) {
                    try {
                        text = Template.parse(parser);
                    } catch (Template.ParseException pe) {
                        throw new LoggingActionException("failed to parse [logging] action. failed to parse text template", pe);
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (CATEGORY_FIELD.match(currentFieldName)) {
                        category = parser.text();
                    } else if (LEVEL_FIELD.match(currentFieldName)) {
                        try {
                            level = LoggingLevel.valueOf(parser.text().toUpperCase(Locale.ROOT));
                        } catch (IllegalArgumentException iae) {
                            throw new LoggingActionException("failed to parse [logging] action. unknown logging level [" + parser.text() + "]");
                        }
                    } else {
                        throw new LoggingActionException("failed to parse [logging] action. unexpected string field [" + currentFieldName + "]");
                    }
                } else {
                    throw new LoggingActionException("failed to parse [logging] action. unexpected token [" + token + "]");
                }
            }

            if (text == null) {
                throw new LoggingActionException("failed to parse [logging] action. missing [text] field");
            }

            return new LoggingAction(logger, text, category, level, settings, templateEngine);
        }

        @Override
        public LoggingAction.Result parseResult(XContentParser parser) throws IOException {
            Boolean success = null;
            String loggedText = null;
            String simulatedLoggedText = null;
            String reason = null;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (LOGGED_TEXT_FIELD.match(currentFieldName)) {
                        loggedText = parser.text();
                    } else if (SIMULATED_LOGGED_TEXT_FIELD.match(currentFieldName)) {
                        simulatedLoggedText = parser.text();
                    } else if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new LoggingActionException("could not parse [logging] result. unexpected string field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                            success = parser.booleanValue();
                        }else {
                            throw new LoggingActionException("could not parse [logging] result. unexpected boolean field [" + currentFieldName + "]");
                        }
                } else {
                    throw new LoggingActionException("could not parse [logging] result. unexpected token [" + token + "]");
                }
            }

            if (success == null) {
                throw new LoggingActionException("could not parse [logging] result. expected boolean field [success]");
            }

            if (simulatedLoggedText != null) {
                return new Result.Simulated(simulatedLoggedText);
            }

            if (success) {
                if (loggedText == null) {
                    throw new LoggingActionException("could not parse successful [logging] result. expected string field [logged_text]");
                }
                return new Result.Success(loggedText);
            }

            if (reason == null) {
                throw new LoggingActionException("could not parse failed [logging] result. expected string field [reason]");
            }

            return new Result.Failure(reason);
        }
    }

    public static class SourceBuilder extends Action.SourceBuilder<SourceBuilder> {

        private Template text;
        private String category;
        private LoggingLevel level;

        public SourceBuilder(Template text) {
            this.text = text;
        }

        @Override
        public String type() {
            return TYPE;
        }

        public SourceBuilder category(String category) {
            this.category = category;
            return this;
        }

        public SourceBuilder level(LoggingLevel level) {
            this.level = level;
            return this;
        }

        @Override
        protected XContentBuilder actionXContent(XContentBuilder builder, Params params) throws IOException {
            if (text == null) {
                throw new ActionException("could not build logging action source. [text] must be defined");
            }
            builder.startObject();
            builder.field(Parser.TEXT_FIELD.getPreferredName(), text);
            if (category != null) {
                builder.field(Parser.CATEGORY_FIELD.getPreferredName(), category);
            }
            if (level != null) {
                builder.field(Parser.LEVEL_FIELD.getPreferredName(), level);
            }
            return builder.endObject();
        }
    }
}
