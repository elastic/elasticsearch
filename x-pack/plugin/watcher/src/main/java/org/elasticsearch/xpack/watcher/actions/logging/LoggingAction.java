/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.logging;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class LoggingAction implements Action {

    public static final String TYPE = "logging";

    final TextTemplate text;
    @Nullable
    final LoggingLevel level;
    @Nullable
    final String category;

    public LoggingAction(TextTemplate text, @Nullable LoggingLevel level, @Nullable String category) {
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoggingAction action = (LoggingAction) o;
        return Objects.equals(text, action.text) && level == action.level && Objects.equals(category, action.category);
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
        builder.field(Field.LEVEL.getPreferredName(), level.value());
        builder.field(Field.TEXT.getPreferredName(), text, params);
        return builder.endObject();
    }

    public static LoggingAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        String category = null;
        LoggingLevel level = null;
        TextTemplate text = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    text = TextTemplate.parse(parser);
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException(
                        "failed to parse [{}] action [{}/{}]. failed to parse [{}] field",
                        pe,
                        TYPE,
                        watchId,
                        actionId,
                        Field.TEXT.getPreferredName()
                    );
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (Field.CATEGORY.match(currentFieldName, parser.getDeprecationHandler())) {
                    category = parser.text();
                } else if (Field.LEVEL.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        level = LoggingLevel.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new ElasticsearchParseException(
                            "failed to parse [{}] action [{}/{}]. unknown logging level [{}]",
                            TYPE,
                            watchId,
                            actionId,
                            parser.text()
                        );
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "failed to parse [{}] action [{}/{}]. unexpected string field [{}]",
                        TYPE,
                        watchId,
                        actionId,
                        currentFieldName
                    );
                }
            } else {
                throw new ElasticsearchParseException(
                    "failed to parse [{}] action [{}/{}]. unexpected token [{}]",
                    TYPE,
                    watchId,
                    actionId,
                    token
                );
            }
        }

        if (text == null) {
            throw new ElasticsearchParseException(
                "failed to parse [{}] action [{}/{}]. missing required [{}] field",
                TYPE,
                watchId,
                actionId,
                Field.TEXT.getPreferredName()
            );
        }

        return new LoggingAction(text, level, category);
    }

    public static Builder builder(TextTemplate template) {
        return new Builder(template);
    }

    public interface Result {

        class Success extends Action.Result implements Result {

            private final String loggedText;

            public Success(String loggedText) {
                super(TYPE, Status.SUCCESS);
                this.loggedText = loggedText;
            }

            public String loggedText() {
                return loggedText;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type).field(Field.LOGGED_TEXT.getPreferredName(), loggedText).endObject();
            }
        }

        class Simulated extends Action.Result implements Result {

            private final String loggedText;

            protected Simulated(String loggedText) {
                super(TYPE, Status.SIMULATED);
                this.loggedText = loggedText;
            }

            public String loggedText() {
                return loggedText;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject(type).field(Field.LOGGED_TEXT.getPreferredName(), loggedText).endObject();
            }
        }
    }

    public static class Builder implements Action.Builder<LoggingAction> {

        final TextTemplate text;
        LoggingLevel level;
        @Nullable
        String category;

        private Builder(TextTemplate text) {
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

    interface Field {
        ParseField CATEGORY = new ParseField("category");
        ParseField LEVEL = new ParseField("level");
        ParseField TEXT = new ParseField("text");
        ParseField LOGGED_TEXT = new ParseField("logged_text");
    }
}
