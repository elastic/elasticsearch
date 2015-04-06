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
import org.elasticsearch.watcher.actions.ActionSettingsException;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.template.ScriptTemplate;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.WatchExecutionContext;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class LoggingAction extends Action<LoggingAction.Result> {

    public static final String TYPE = "logging";

    private final String category;
    private final LoggingLevel level;
    private final Template template;

    private final ESLogger actionLogger;

    public LoggingAction(ESLogger logger, ESLogger actionLogger, @Nullable String category, LoggingLevel level, Template template) {
        super(logger);
        this.category = category;
        this.level = level;
        this.template = template;
        this.actionLogger = actionLogger;
    }

    @Override
    public String type() {
        return TYPE;
    }

    String category() {
        return category;
    }

    LoggingLevel level() {
        return level;
    }

    Template template() {
        return template;
    }

    ESLogger logger() {
        return actionLogger;
    }

    @Override
    protected LoggingAction.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        try {
            String text = template.render(Variables.createCtxModel(ctx, payload));
            level.log(actionLogger, text);
            return new Result.Success(text);
        } catch (Exception e) {
            return new Result.Failure("failed to execute log action: " + e.getMessage());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (category != null) {
            builder.field(Parser.CATEGORY_FIELD.getPreferredName(), category);
        }
        builder.field(Parser.LEVEL_FIELD.getPreferredName(), level);
        builder.field(Parser.TEXT_FIELD.getPreferredName(), template);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LoggingAction action = (LoggingAction) o;

        if (category != null ? !category.equals(action.category) : action.category != null) return false;
        if (level != action.level) return false;
        return template.equals(action.template);
    }

    @Override
    public int hashCode() {
        int result = category != null ? category.hashCode() : 0;
        result = 31 * result + level.hashCode();
        result = 31 * result + template.hashCode();
        return result;
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
    }

    public static class Parser implements Action.Parser<LoggingAction.Result, LoggingAction> {

        static final ParseField CATEGORY_FIELD = new ParseField("category");
        static final ParseField LEVEL_FIELD = new ParseField("level");
        static final ParseField TEXT_FIELD = new ParseField("text");
        static final ParseField LOGGED_TEXT_FIELD = new ParseField("logged_text");
        static final ParseField REASON_FIELD = new ParseField("reason");

        private final Settings settings;
        private final Template.Parser templateParser;
        private final ESLogger logger;

        @Inject
        public Parser(Settings settings, Template.Parser templateParser) {
            this.settings = settings;
            this.logger = Loggers.getLogger(LoggingAction.class, settings);
            this.templateParser = templateParser;
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
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (CATEGORY_FIELD.match(currentFieldName)) {
                        category = parser.text();
                    } else if (LEVEL_FIELD.match(currentFieldName)) {
                        try {
                            level = LoggingLevel.valueOf(parser.text().toUpperCase(Locale.ROOT));
                        } catch (IllegalArgumentException iae) {
                            throw new ActionSettingsException("failed to parse logging action. unknown logging level [" + parser.text() + "]");
                        }
                    } else if (TEXT_FIELD.match(currentFieldName)) {
                        try {
                            text = templateParser.parse(parser);
                        } catch (Template.Parser.ParseException pe) {
                            throw new ActionSettingsException("failed to parse logging action. failed to parse text template", pe);
                        }
                    } else {
                        throw new ActionSettingsException("failed to parse logging action. unexpected string field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (TEXT_FIELD.match(currentFieldName)) {
                        try {
                            text = templateParser.parse(parser);
                        } catch (Template.Parser.ParseException pe) {
                            throw new ActionSettingsException("failed to parse logging action. failed to parse text template", pe);
                        }
                    } else {
                        throw new ActionSettingsException("failed to parse logging action. unexpected object field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ActionSettingsException("failed to parse logging action. unexpected token [" + token + "]");
                }
            }

            if (text == null) {
                throw new ActionSettingsException("failed to parse logging action. missing [text] field");
            }

            ESLogger actionLogger = category != null ? Loggers.getLogger(category, settings) : logger;

            return new LoggingAction(logger, actionLogger, category, level, text);
        }

        @Override
        public LoggingAction.Result parseResult(XContentParser parser) throws IOException {
            Boolean success = null;
            String loggedText = null;
            String reason = null;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (LOGGED_TEXT_FIELD.match(currentFieldName)) {
                        loggedText = parser.text();
                    } else if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new ActionException("could not parse index result. unexpected string field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                            success = parser.booleanValue();
                        } else {
                            throw new ActionException("could not parse index result. unexpected boolean field [" + currentFieldName + "]");
                        }
                } else {
                    throw new ActionException("could not parse index result. unexpected token [" + token + "]");
                }
            }

            if (success == null) {
                throw new ActionException("could not parse index result. expected boolean field [success]");
            }

            if (success) {
                if (loggedText == null) {
                    throw new ActionException("could not parse successful index result. expected string field [logged_text]");
                }
                return new Result.Success(loggedText);
            }

            if (reason == null) {
                throw new ActionException("could not parse failed index result. expected string field [reason]");
            }

            return new Result.Failure(reason);
        }
    }

    public static class SourceBuilder extends Action.SourceBuilder<SourceBuilder> {

        private Template.SourceBuilder text;
        private String category;
        private LoggingLevel level;

        public SourceBuilder(String id) {
            super(id);
        }

        @Override
        public String type() {
            return TYPE;
        }

        public SourceBuilder text(String text) {
            return text(new ScriptTemplate.SourceBuilder(text));
        }

        public SourceBuilder text(Template.SourceBuilder text) {
            this.text = text;
            return this;
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
