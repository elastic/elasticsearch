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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_BOOST;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_PREFIX;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_VALUE;

/**
 * Defines the query context for {@link CategoryContextMapping}
 */
public final class CategoryQueryContext implements ToXContent {
    private final CharSequence category;
    private final boolean isPrefix;
    private final int boost;

    private CategoryQueryContext(CharSequence category, int boost, boolean isPrefix) {
        this.category = category;
        this.boost = boost;
        this.isPrefix = isPrefix;
    }

    /**
     * Returns the category of the context
     */
    public CharSequence getCategory() {
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

    public static class Builder {
        private CharSequence category;
        private boolean isPrefix = false;
        private int boost = 1;

        public Builder() {
        }

        /**
         * Sets the category of the context.
         * This is a required field
         */
        public Builder setCategory(CharSequence context) {
            this.category = context;
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
            this.boost = boost;
            return this;
        }

        public CategoryQueryContext build() {
            return new CategoryQueryContext(category, boost, isPrefix);
        }
    }

    private static ObjectParser<Builder, Void> CATEGORY_PARSER = new ObjectParser<>("category", null);
    static {
        CATEGORY_PARSER.declareString(Builder::setCategory, new ParseField("context"));
        CATEGORY_PARSER.declareInt(Builder::setBoost, new ParseField("boost"));
        CATEGORY_PARSER.declareBoolean(Builder::setPrefix, new ParseField("prefix"));
    }

    public static CategoryQueryContext parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Builder builder = builder();
        if (token == XContentParser.Token.START_OBJECT) {
            CATEGORY_PARSER.parse(parser, builder);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            builder.setCategory(parser.text());
        } else {
            throw new ElasticsearchParseException("category context must be an object or string");
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
}
