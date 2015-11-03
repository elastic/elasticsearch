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
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;

import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_BOOST;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_PREFIX;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_VALUE;

/**
 * Defines the query context for {@link CategoryContextMapping}
 */
public final class CategoryQueryContext implements ToXContent {

    public CharSequence context;

    public boolean isPrefix = false;

    public int boost = 1;

    /**
     * Creates a query context with a provided context and a
     * boost of 1
     */
    public CategoryQueryContext(CharSequence context) {
        this(context, 1);
    }

    /**
     * Creates a query context with a provided context and boost
     */
    public CategoryQueryContext(CharSequence context, int boost) {
        this(context, boost, false);
    }

    /**
     * Creates a query context with a provided context and boost
     * Allows specifying whether the context should be treated as
     * a prefix or not
     */
    public CategoryQueryContext(CharSequence context, int boost, boolean isPrefix) {
        this.context = context;
        this.boost = boost;
        this.isPrefix = isPrefix;
    }

    private CategoryQueryContext() {
    }

    void setContext(CharSequence context) {
        this.context = context;
    }

    void setIsPrefix(boolean isPrefix) {
        this.isPrefix = isPrefix;
    }

    void setBoost(int boost) {
        this.boost = boost;
    }

    private static ObjectParser<CategoryQueryContext, CategoryContextMapping> CATEGORY_PARSER = new ObjectParser<>("category", null);
    static {
        CATEGORY_PARSER.declareString(CategoryQueryContext::setContext, new ParseField("context"));
        CATEGORY_PARSER.declareInt(CategoryQueryContext::setBoost, new ParseField("boost"));
        CATEGORY_PARSER.declareBoolean(CategoryQueryContext::setIsPrefix, new ParseField("prefix"));
    }

    public static CategoryQueryContext parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        CategoryQueryContext queryContext = new CategoryQueryContext();
        if (token == XContentParser.Token.START_OBJECT) {
            CATEGORY_PARSER.parse(parser, queryContext);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            queryContext.setContext(parser.text());
        } else {
            throw new ElasticsearchParseException("category context must be an object or string");
        }
        return queryContext;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONTEXT_VALUE, context);
        builder.field(CONTEXT_BOOST, boost);
        builder.field(CONTEXT_PREFIX, isPrefix);
        builder.endObject();
        return builder;
    }
}
