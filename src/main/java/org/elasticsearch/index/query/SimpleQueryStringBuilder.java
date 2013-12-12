/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * SimpleQuery is a query parser that acts similar to a query_string
 * query, but won't throw exceptions for any weird string syntax.
 */
public class SimpleQueryStringBuilder extends BaseQueryBuilder {
    private Map<String, Float> fields = new HashMap<String, Float>();
    private String analyzer;
    private Operator operator;
    private final String queryText;

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

        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }

        if (operator != null) {
            builder.field("default_operator", operator.name().toLowerCase(Locale.ROOT));
        }

        builder.endObject();
    }
}
