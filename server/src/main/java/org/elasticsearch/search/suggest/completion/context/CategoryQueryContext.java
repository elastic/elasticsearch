/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion.context;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_BOOST;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_PREFIX;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_VALUE;

/**
 * Defines the query context for {@link CategoryContextMapping}
 */
public final class CategoryQueryContext implements ToXContentObject {
    public static final String NAME = "category";

    private final String category;
    private final boolean isPrefix;
    private final int boost;

    private CategoryQueryContext(String category, int boost, boolean isPrefix) {
        this.category = category;
        this.boost = boost;
        this.isPrefix = isPrefix;
    }

    /**
     * Returns the category of the context
     */
    public String getCategory() {
        return category;
    }

    /**
     * Returns if the context should be treated as a prefix
     */
    public boolean isPrefix() {
        return isPrefix;
    }

    /**
     * Returns the query-time boost of the context
     */
    public int getBoost() {
        return boost;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CategoryQueryContext that = (CategoryQueryContext) o;

        if (isPrefix != that.isPrefix) return false;
        if (boost != that.boost) return false;
        return category != null ? category.equals(that.category) : that.category == null;

    }

    @Override
    public int hashCode() {
        int result = category != null ? category.hashCode() : 0;
        result = 31 * result + (isPrefix ? 1 : 0);
        result = 31 * result + boost;
        return result;
    }

    private static final ObjectParser<Builder, Void> CATEGORY_PARSER = new ObjectParser<>(NAME);
    static {
        CATEGORY_PARSER.declareField(
            Builder::setCategory,
            XContentParser::text,
            new ParseField(CONTEXT_VALUE),
            ObjectParser.ValueType.VALUE
        );
        CATEGORY_PARSER.declareInt(Builder::setBoost, new ParseField(CONTEXT_BOOST));
        CATEGORY_PARSER.declareBoolean(Builder::setPrefix, new ParseField(CONTEXT_PREFIX));
    }

    public static CategoryQueryContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Builder builder = builder();
        if (token == XContentParser.Token.START_OBJECT) {
            try {
                CATEGORY_PARSER.parse(parser, builder, null);
            } catch (XContentParseException e) {
                throw new XContentParseException("category context must be a string, number or boolean");
            }
        } else if (token == XContentParser.Token.VALUE_STRING
            || token == XContentParser.Token.VALUE_BOOLEAN
            || token == XContentParser.Token.VALUE_NUMBER) {
                builder.setCategory(parser.text());
            } else {
                throw new XContentParseException("category context must be an object, string, number or boolean");
            }
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONTEXT_VALUE, category);
        builder.field(CONTEXT_BOOST, boost);
        builder.field(CONTEXT_PREFIX, isPrefix);
        builder.endObject();
        return builder;
    }

    public static class Builder {
        private String category;
        private boolean isPrefix = false;
        private int boost = 1;

        public Builder() {}

        /**
         * Sets the category of the category.
         * This is a required field
         */
        public Builder setCategory(String category) {
            Objects.requireNonNull(category, "category must not be null");
            this.category = category;
            return this;
        }

        /**
         * Sets if the context should be treated as a prefix or not.
         * Defaults to false
         */
        public Builder setPrefix(boolean prefix) {
            this.isPrefix = prefix;
            return this;
        }

        /**
         * Sets the query-time boost of the context.
         * Defaults to 1.
         */
        public Builder setBoost(int boost) {
            if (boost <= 0) {
                throw new IllegalArgumentException("boost must be greater than 0");
            }
            this.boost = boost;
            return this;
        }

        public CategoryQueryContext build() {
            Objects.requireNonNull(category, "category must not be null");
            return new CategoryQueryContext(category, boost, isPrefix);
        }
    }
}
