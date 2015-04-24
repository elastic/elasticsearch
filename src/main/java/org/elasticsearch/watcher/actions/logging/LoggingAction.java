/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.support.template.Template;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class LoggingAction implements Action {

    public static final String TYPE = "logging";

    final Template text;
    final @Nullable LoggingLevel level;
    final @Nullable String category;

    public LoggingAction(Template text, @Nullable LoggingLevel level, @Nullable String category) {
        this.text = text;
        this.level = level != null ? level : LoggingLevel.INFO;
        this.category = category;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LoggingAction action = (LoggingAction) o;

        if (!text.equals(action.text)) return false;
        if (level != action.level) return false;
        return !(category != null ? !category.equals(action.category) : action.category != null);
    }

    @Override
    public int hashCode() {
        int result = text.hashCode();
        result = 31 * result + (level != null ? level.hashCode() : 0);
        result = 31 * result + (category != null ? category.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (category != null) {
            builder.field(Field.CATEGORY.getPreferredName(), category);
        }
        builder.field(Field.LEVEL.getPreferredName(), level, params);
        builder.field(Field.TEXT.getPreferredName(), text, params);
        return builder.endObject();
    }

    public static LoggingAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String category = null;
        LoggingLevel level = null;
        Template text = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TEXT.match(currentFieldName)) {
                try {
                    text = Template.parse(parser);
                } catch (Template.ParseException pe) {
                    throw new LoggingActionException("failed to parse [{}] action [{}/{}]. failed to parse [{}] field", pe, TYPE, watchId, actionId, Field.TEXT.getPreferredName());
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.CATEGORY.match(currentFieldName)) {
                    category = parser.text();
                } else if (Field.LEVEL.match(currentFieldName)) {
                    try {
                        level = LoggingLevel.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new LoggingActionException("failed to parse [{}] action [{}/{}]. unknown logging level [{}]", TYPE, watchId, actionId, parser.text());
                    }
                } else {
                    throw new LoggingActionException("failed to parse [{}] action [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                }
            } else {
                throw new LoggingActionException("failed to parse [{}] action [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
            }
        }

        if (text == null) {
            throw new LoggingActionException("failed to parse [{}] action [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.TEXT.getPreferredName());
        }

        return new LoggingAction(text, level, category);
    }

    public static Builder builder(Template template) {
        return new Builder(template);
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
                return builder.field(Field.LOGGED_TEXT.getPreferredName(), loggedText);
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
                return builder.field(Field.REASON.getPreferredName(), reason);
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
                return builder.field(Field.SIMULATED_LOGGED_TEXT.getPreferredName(), loggedText);
            }
        }

        public static Result parse(String watchId, String actionId, XContentParser parser) throws IOException {
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
                    if (Field.LOGGED_TEXT.match(currentFieldName)) {
                        loggedText = parser.text();
                    } else if (Field.SIMULATED_LOGGED_TEXT.match(currentFieldName)) {
                        simulatedLoggedText = parser.text();
                    } else if (Field.REASON.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new LoggingActionException("could not parse [{}] action result [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Field.SUCCESS.match(currentFieldName)) {
                        success = parser.booleanValue();
                    } else {
                        throw new LoggingActionException("could not parse [{}] action result [{}/{}]. unexpected boolean field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else {
                    throw new LoggingActionException("could not parse [{}] action result [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
                }
            }

            if (success == null) {
                throw new LoggingActionException("could not parse [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.SUCCESS.getPreferredName());
            }

            if (simulatedLoggedText != null) {
                return new LoggingAction.Result.Simulated(simulatedLoggedText);
            }

            if (success) {
                if (loggedText == null) {
                    throw new LoggingActionException("could not parse successful [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.LOGGED_TEXT.getPreferredName());
                }
                return new LoggingAction.Result.Success(loggedText);
            }

            if (reason == null) {
                throw new LoggingActionException("could not parse failed [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.REASON.getPreferredName());
            }
            return new LoggingAction.Result.Failure(reason);
        }
    }

    public static class Builder implements Action.Builder<LoggingAction> {

        final Template text;
        LoggingLevel level;
        @Nullable String category;

        private Builder(Template text) {
            this.text = text;
        }

        public Builder setLevel(LoggingLevel level) {
            this.level = level;
            return this;
        }

        public Builder setCategory(String category) {
            this.category = category;
            return this;
        }

        @Override
        public LoggingAction build() {
            return new LoggingAction(text, level, category);
        }
    }

    interface Field extends Action.Field {
        ParseField CATEGORY = new ParseField("category");
        ParseField LEVEL = new ParseField("level");
        ParseField TEXT = new ParseField("text");
        ParseField LOGGED_TEXT = new ParseField("logged_text");
        ParseField SIMULATED_LOGGED_TEXT = new ParseField("simulated_logged_text");
    }
}
