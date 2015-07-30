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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.xdocument.CompletionQuery;
import org.apache.lucene.search.suggest.xdocument.ContextQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;

import java.io.IOException;
import java.util.*;

/**
 * A {@link ContextMapping} that defines criterion type as a category
 * The suggestions are boosted and/or filtered by their associated
 * category value.
 * {@link CategoryQueryContext} defines the options for constructing
 * a unit of query context for this context type
 *
 * Internally, category values are prepended with the suggestion
 * value at index time
 */
public class CategoryContextMapping extends ContextMapping<CategoryQueryContext> {

    private static final String FIELD_FIELDNAME = "path";

    static final String CONTEXT_VALUE = "context";
    static final String CONTEXT_BOOST = "boost";
    static final String CONTEXT_PREFIX = "prefix";

    private final String fieldName;

    /**
     * Create a new {@link CategoryContextMapping} with field
     * <code>fieldName</code>
     */
    private CategoryContextMapping(String name, String fieldName) {
        super(Type.CATEGORY, name);
        this.fieldName = fieldName;
    }

    /**
     * Name of the field to get contexts from at index-time
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Loads a <code>name</code>d {@link CategoryContextMapping} instance
     * from a map.
     * see {@link ContextMappings#load(Object, Version)}
     *
     * Acceptable map param: <code>path</code>
     */
    protected static CategoryContextMapping load(String name, Map<String, Object> config) throws ElasticsearchParseException {
        CategoryContextMapping.Builder mapping = new CategoryContextMapping.Builder(name);
        Object fieldName = config.get(FIELD_FIELDNAME);
        if (fieldName != null) {
            mapping.field(fieldName.toString());
            config.remove(FIELD_FIELDNAME);
        }
        return mapping.build();
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName != null) {
            builder.field(FIELD_FIELDNAME, fieldName);
        }
        return builder;
    }

    /**
     * Parse a set of {@link CharSequence} contexts at index-time.
     * Acceptable formats:
     *
     *  <ul>
     *     <li>Array: <pre>[<i>&lt;string&gt;</i>, ..]</pre></li>
     *     <li>String: <pre>&quot;string&quot;</pre></li>
     *  </ul>
     */
    @Override
    public Set<CharSequence> parseContext(ParseContext parseContext, XContentParser parser) throws IOException, ElasticsearchParseException {
        final Set<CharSequence> contexts = new HashSet<>();
        Token token = parser.currentToken();
        if (token == Token.VALUE_STRING) {
            contexts.add(parser.text());
        } else if (token == Token.START_ARRAY) {
            while(parser.nextToken() != Token.END_ARRAY) {
                contexts.add(parser.text());
            }
        } else {
            throw new ElasticsearchParseException("Category contexts must be a string or a list of strings");
        }
        return contexts;
    }

    @Override
    public Set<CharSequence> parseContext(Document document) {
        Set<CharSequence> values = null;
        if (fieldName != null) {
            IndexableField[] fields = document.getFields(fieldName);
            values = new HashSet<>(fields.length);
            for (IndexableField field : fields) {
                values.add(field.stringValue());
            }
        }
        return (values == null) ? new HashSet<CharSequence>(0) : values;
    }

    /**
     * Parse a {@link QueryContexts<CategoryQueryContext>}
     * with a {@link XContentParser}. A QueryContexts accepts one of the following forms:
     *
     * <ul>
     *     <li>Object: CategoryQueryContext</li>
     *     <li>String: CategoryQueryContext value with exact=true and boost=1</li>
     *     <li>Array: <pre>[CategoryQueryContext, ..]</pre></li>
     * </ul>
     *
     *  A {@link CategoryQueryContext} has one of the following forms:
     *  <ul>
     *     <li>Object: <pre>{&quot;value&quot;: <i>&lt;string&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;exact&quot;: <i>&lt;boolean&gt;</i>}</pre></li>
     *     <li>String: <pre>&quot;string&quot;</pre></li>
     *  </ul>
     */
    @Override
    public QueryContexts<CategoryQueryContext> parseQueryContext(String name, XContentParser parser) throws IOException, ElasticsearchParseException {
        final QueryContexts<CategoryQueryContext> queryContexts = new QueryContexts<>(name);
        Token token = parser.nextToken();
        if (token == Token.START_OBJECT || token == Token.VALUE_STRING) {
            queryContexts.add(innerParseQueryContext(parser));
        } else if (token == Token.START_ARRAY) {
            while (parser.nextToken() != Token.END_ARRAY) {
                queryContexts.add(innerParseQueryContext(parser));
            }
        }
        return queryContexts;
    }

    private CategoryQueryContext innerParseQueryContext(XContentParser parser) throws IOException, ElasticsearchParseException {
        Token token = parser.currentToken();
        if (token == Token.VALUE_STRING) {
            return new CategoryQueryContext(parser.text());
        } else if (token == Token.START_OBJECT) {
            String currentFieldName = null;
            String context = null;
            boolean isPrefix = false;
            int boost = 1;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == Token.VALUE_STRING) {
                    // context, exact
                    if (CONTEXT_VALUE.equals(currentFieldName)) {
                        context = parser.text();
                    } else if (CONTEXT_PREFIX.equals(currentFieldName)) {
                        isPrefix = Boolean.valueOf(parser.text());
                    } else if (CONTEXT_BOOST.equals(currentFieldName)) {
                        Number number;
                        try {
                            number = Long.parseLong(parser.text());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Boost must be a string representing a numeric value, but was [" + parser.text() + "]");
                        }
                        boost = number.intValue();
                    }
                } else if (token == Token.VALUE_NUMBER) {
                    // boost
                    if (CONTEXT_BOOST.equals(currentFieldName)) {
                        Number number = parser.numberValue();
                        if (parser.numberType() == XContentParser.NumberType.INT) {
                            boost = number.intValue();
                        } else {
                            throw new ElasticsearchParseException("Boost must be in the interval [0..2147483647], but was [" + number.longValue() + "]");
                        }
                    }
                } else if (token == Token.VALUE_BOOLEAN) {
                    // exact
                    if (CONTEXT_PREFIX.equals(currentFieldName)) {
                        isPrefix = parser.booleanValue();
                    }
                }
            }
            if (context == null) {
                throw new ElasticsearchParseException("no context provided");
            }
            return new CategoryQueryContext(context, boost, isPrefix);
        } else {
            throw new ElasticsearchParseException("expected string or object");
        }
    }

    @Override
    protected CompletionQuery toContextQuery(CompletionQuery query, @Nullable QueryContexts<CategoryQueryContext> queryContexts) {
        final ContextQuery contextQuery = new ContextQuery(query);
        if (queryContexts != null) {
            for (CategoryQueryContext queryContext : queryContexts) {
                contextQuery.addContext(queryContext.context, queryContext.boost, queryContext.isPrefix == false);
            }
        }
        return contextQuery;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            if (obj instanceof CategoryContextMapping) {
                CategoryContextMapping other = (CategoryContextMapping) obj;
                return (this.fieldName.equals(other.fieldName));
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        return result;
    }

    /**
     * Builder for {@link CategoryContextMapping}
     */
    public static class Builder extends ContextBuilder<CategoryContextMapping> {

        private String fieldName;

        /**
         * Create a builder for
         * a named {@link CategoryContextMapping}
         * @param name name of the mapping
         */
        public Builder(String name) {
            super(name);
        }

        /**
         * Set the name of the field to use
         */
        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public CategoryContextMapping build() {
            return new CategoryContextMapping(name, fieldName);
        }
    }
}
