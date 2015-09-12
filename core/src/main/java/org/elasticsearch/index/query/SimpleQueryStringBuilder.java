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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * SimpleQuery is a query parser that acts similar to a query_string
 * query, but won't throw exceptions for any weird string syntax.
 */
public class SimpleQueryStringBuilder extends QueryBuilder implements BoostableQueryBuilder<SimpleQueryStringBuilder> {
    private Map<String, Float> fields = new HashMap<>();
    private String analyzer;
    private Operator operator;
    private final String queryText;
    private String queryName;
    private String minimumShouldMatch;
    private int flags = -1;
    private float boost = -1.0f;
    private Boolean lowercaseExpandedTerms;
    private Boolean lenient;
    private Boolean analyzeWildcard;
    private Locale locale;

    /**
     * Operators for the default_operator
     */
    public static enum Operator {
        AND,
        OR
    }

    /**
     * Construct a new simple query with the given text
     */
    public SimpleQueryStringBuilder(String text) {
        this.queryText = text;
    }

    /** Set the boost of this query. */
    @Override
    public SimpleQueryStringBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }
    
    /** Returns the boost of this query. */
    public float boost() {
        return this.boost;
    }

    /**
     * Add a field to run the query against
     */
    public SimpleQueryStringBuilder field(String field) {
        this.fields.put(field, null);
        return this;
    }

    /**
     * Add a field to run the query against with a specific boost
     */
    public SimpleQueryStringBuilder field(String field, float boost) {
        this.fields.put(field, boost);
        return this;
    }

    /**
     * Specify a name for the query
     */
    public SimpleQueryStringBuilder queryName(String name) {
        this.queryName = name;
        return this;
    }

    /**
     * Specify an analyzer to use for the query
     */
    public SimpleQueryStringBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Specify the default operator for the query. Defaults to "OR" if no
     * operator is specified
     */
    public SimpleQueryStringBuilder defaultOperator(Operator defaultOperator) {
        this.operator = defaultOperator;
        return this;
    }

    /**
     * Specify the enabled features of the SimpleQueryString.
     */
    public SimpleQueryStringBuilder flags(SimpleQueryStringFlag... flags) {
        int value = 0;
        if (flags.length == 0) {
            value = SimpleQueryStringFlag.ALL.value;
        } else {
            for (SimpleQueryStringFlag flag : flags) {
                value |= flag.value;
            }
        }
        this.flags = value;
        return this;
    }

    public SimpleQueryStringBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    public SimpleQueryStringBuilder locale(Locale locale) {
        this.locale = locale;
        return this;
    }

    public SimpleQueryStringBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    public SimpleQueryStringBuilder analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        return this;
    }

    public SimpleQueryStringBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SimpleQueryStringParser.NAME);

        builder.field("query", queryText);

        if (fields.size() > 0) {
            builder.startArray("fields");
            for (Map.Entry<String, Float> entry : fields.entrySet()) {
                String field = entry.getKey();
                Float boost = entry.getValue();
                if (boost != null) {
                    builder.value(field + "^" + boost);
                } else {
                    builder.value(field);
                }
            }
            builder.endArray();
        }

        if (flags != -1) {
            builder.field("flags", flags);
        }

        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }

        if (operator != null) {
            builder.field("default_operator", operator.name().toLowerCase(Locale.ROOT));
        }

        if (lowercaseExpandedTerms != null) {
            builder.field("lowercase_expanded_terms", lowercaseExpandedTerms);
        }

        if (lenient != null) {
            builder.field("lenient", lenient);
        }

        if (analyzeWildcard != null) {
            builder.field("analyze_wildcard", analyzeWildcard);
        }

        if (locale != null) {
            builder.field("locale", locale.toString());
        }

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        
        if (boost != -1.0f) {
            builder.field("boost", boost);
        }

        builder.endObject();
    }

}
