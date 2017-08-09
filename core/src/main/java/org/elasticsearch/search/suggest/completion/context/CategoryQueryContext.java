/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest.completion.context;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

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

    private static ObjectParser<Builder, Void> CATEGORY_PARSER = new ObjectParser<>(NAME, null);
    static {
        CATEGORY_PARSER.declareField(Builder::setCategory, XContentParser::text, new ParseField(CONTEXT_VALUE),
                ObjectParser.ValueType.VALUE);
        CATEGORY_PARSER.declareInt(Builder::setBoost, new ParseField(CONTEXT_BOOST));
        CATEGORY_PARSER.declareBoolean(Builder::setPrefix, new ParseField(CONTEXT_PREFIX));
    }

    public static CategoryQueryContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Builder builder = builder();
        if (token == XContentParser.Token.START_OBJECT) {
            try {
                CATEGORY_PARSER.parse(parser, builder, null);
            } catch(ParsingException e) {
                throw new ElasticsearchParseException("category context must be a string, number or boolean");
            }
        } else if (token == XContentParser.Token.VALUE_STRING || token == XContentParser.Token.VALUE_BOOLEAN
                || token == XContentParser.Token.VALUE_NUMBER) {
            builder.setCategory(parser.text());
        } else {
            throw new ElasticsearchParseException("category context must be an object, string, number or boolean");
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

        public Builder() {
        }

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
