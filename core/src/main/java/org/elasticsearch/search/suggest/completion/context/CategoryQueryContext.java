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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_BOOST;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_PREFIX;
import static org.elasticsearch.search.suggest.completion.context.CategoryContextMapping.CONTEXT_VALUE;

/**
 * Defines the query context for {@link CategoryContextMapping}
 */
public class CategoryQueryContext implements ToXContent {
    /**
     * The value of the category
     */
    public final CharSequence context;

    /**
     * Whether the value is a
     * prefix of a category value or not
     */
    public final boolean isPrefix;

    /**
     * Query-time boost to be
     * applied to suggestions associated
     * with this category
     */
    public final int boost;

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
