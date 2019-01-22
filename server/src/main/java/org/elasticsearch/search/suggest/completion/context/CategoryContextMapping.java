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

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.StringFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link ContextMapping} that uses a simple string as a criteria
 * The suggestions are boosted and/or filtered by their associated
 * category (string) value.
 * {@link CategoryQueryContext} defines options for constructing
 * a unit of query context for this context type
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
    public Set<String> parseContext(ParseContext parseContext, XContentParser parser)
            throws IOException, ElasticsearchParseException {
        final Set<String> contexts = new HashSet<>();
        Token token = parser.currentToken();
        if (token == Token.VALUE_STRING || token == Token.VALUE_NUMBER || token == Token.VALUE_BOOLEAN) {
            contexts.add(parser.text());
        } else if (token == Token.START_ARRAY) {
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                if (token == Token.VALUE_STRING || token == Token.VALUE_NUMBER || token == Token.VALUE_BOOLEAN) {
                    contexts.add(parser.text());
                } else {
                    throw new ElasticsearchParseException(
                            "context array must have string, number or boolean values, but was [" + token + "]");
                }
            }
        } else {
            throw new ElasticsearchParseException(
                    "contexts must be a string, number or boolean or a list of string, number or boolean, but was [" + token + "]");
        }
        return contexts;
    }

    @Override
    public Set<String> parseContext(Document document) {
        Set<String> values = null;
        if (fieldName != null) {
            IndexableField[] fields = document.getFields(fieldName);
            values = new HashSet<>(fields.length);
            for (IndexableField field : fields) {
                if (field instanceof SortedDocValuesField ||
                        field instanceof SortedSetDocValuesField ||
                        field instanceof StoredField) {
                    // Ignore doc values and stored fields
                } else if (field.fieldType() instanceof KeywordFieldMapper.KeywordFieldType) {
                    values.add(field.binaryValue().utf8ToString());
                } else if (field.fieldType() instanceof StringFieldType) {
                    values.add(field.stringValue());
                } else {
                    throw new IllegalArgumentException("Failed to parse context field [" + fieldName +
                            "], only keyword and text fields are accepted");
                }
            }
        }
        return (values == null) ? Collections.emptySet() : values;
    }

    @Override
    protected CategoryQueryContext fromXContent(XContentParser parser) throws IOException {
        return CategoryQueryContext.fromXContent(parser);
    }

    /**
     * Parse a list of {@link CategoryQueryContext}
     * using <code>parser</code>. A QueryContexts accepts one of the following forms:
     *
     * <ul>
     *     <li>Object: CategoryQueryContext</li>
     *     <li>String: CategoryQueryContext value with prefix=false and boost=1</li>
     *     <li>Array: <pre>[CategoryQueryContext, ..]</pre></li>
     * </ul>
     *
     *  A CategoryQueryContext has one of the following forms:
     *  <ul>
     *     <li>Object: <pre>{&quot;context&quot;: <i>&lt;string&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;prefix&quot;:
     *     <i>&lt;boolean&gt;</i>}</pre></li>
     *     <li>String: <pre>&quot;string&quot;</pre></li>
     *  </ul>
     */
    @Override
    public List<InternalQueryContext> toInternalQueryContexts(List<CategoryQueryContext> queryContexts) {
        List<InternalQueryContext> internalInternalQueryContexts = new ArrayList<>(queryContexts.size());
        internalInternalQueryContexts.addAll(
            queryContexts.stream()
                .map(queryContext -> new InternalQueryContext(queryContext.getCategory(), queryContext.getBoost(), queryContext.isPrefix()))
                .collect(Collectors.toList()));
        return internalInternalQueryContexts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CategoryContextMapping mapping = (CategoryContextMapping) o;
        return !(fieldName != null ? !fieldName.equals(mapping.fieldName) : mapping.fieldName != null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldName);
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
