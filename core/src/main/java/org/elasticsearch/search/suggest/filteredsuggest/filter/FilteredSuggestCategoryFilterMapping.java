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

package org.elasticsearch.search.suggest.filteredsuggest.filter;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.InternalQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The {@link FilteredSuggestCategoryFilterMapping} is used to define a {@link FilteredSuggestFilterMapping} that
 * references a field within a document. The value of the field in turn will be used to setup the suggestions made by
 * the completion suggester.
 */
public class FilteredSuggestCategoryFilterMapping extends FilteredSuggestFilterMapping<FilteredSuggestCategoryQueryContext> {

    protected static final String FILTER_FIELD_PATH = "path";

    private final String fieldName;
    private final String fieldIds;
    private final FilteredSuggestFilterValueGenerator defaultConfig;

    /**
     * Create a new {@link FilteredSuggestCategoryFilterMapping} with the default field <code>[_type]</code>
     */
    public FilteredSuggestCategoryFilterMapping(String name, String fieldName, String fieldIds, boolean persist, boolean indexFilterValue) {
        super(Type.CATEGORY, name, persist);
        this.fieldName = fieldName;
        this.fieldIds = fieldIds;
        this.defaultConfig = new FilteredSuggestFilterValueGenerator(fieldName, fieldIds, null, indexFilterValue);
    }

    /**
     * Name of the field used by this {@link FilteredSuggestCategoryFilterMapping}
     */
    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    protected String getFieldIds() {
        return fieldIds;
    }

    @Override
    public FilteredSuggestFilterValues defaultConfig() {
        return defaultConfig;
    }

    /**
     * Load the specification of a {@link FilteredSuggestCategoryFilterMapping}
     * 
     * @param name
     *            of the field to use. If <code>null</code> default field will be used
     * 
     * @return new {@link FilteredSuggestCategoryFilterMapping}
     */
    protected static FilteredSuggestCategoryFilterMapping load(String name, Map<String, Object> config, boolean persist,
            boolean indexFilterValue) throws ElasticsearchParseException {
        FilteredSuggestCategoryFilterMapping.Builder mapping = new FilteredSuggestCategoryFilterMapping.Builder(name);

        Object filterFieldPath = config.get(FILTER_FIELD_PATH);

        if (filterFieldPath != null) {
            mapping.filterFieldPath(filterFieldPath.toString());
        }

        mapping.setFilterFieldIds(config.get(FilterMappings.FILTER_ID).toString());
        mapping.persist(persist);
        mapping.indexFilterValue(indexFilterValue);

        return mapping.build();
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName != null) {
            builder.field(FILTER_FIELD_PATH, fieldName);
        }

        return builder;
    }

    @Override
    protected FilteredSuggestCategoryQueryContext fromXContent(QueryParseContext context) throws IOException {
        return FilteredSuggestCategoryQueryContext.fromXContent(context, fieldIds);
    }

    /**
     * At index time
     */
    @Override
    public FilteredSuggestFilterValues parseContext(ParseContext parseContext, XContentParser parser)
            throws IOException, ElasticsearchParseException {
        Token token = parser.currentToken();
        if (token == Token.VALUE_NULL) {
            return new FilteredSuggestFilterValueGenerator(fieldName, fieldIds, null, false);
        } else if (token == Token.VALUE_STRING) {
            return new FilteredSuggestFilterValueGenerator(fieldName, fieldIds, Collections.singleton(parser.text()), false);
        } else if (token == Token.VALUE_NUMBER) {
            return new FilteredSuggestFilterValueGenerator(fieldName, fieldIds, Collections.singleton(parser.text()), false);
        } else if (token == Token.VALUE_BOOLEAN) {
            return new FilteredSuggestFilterValueGenerator(fieldName, fieldIds, Collections.singleton(parser.text()), false);
        } else if (token == Token.START_ARRAY) {
            ArrayList<String> values = new ArrayList<>();
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                values.add(parser.text());
            }
            if (values.isEmpty()) {
                throw new ElasticsearchParseException("FieldConfig must contain a least one category");
            }
            return new FilteredSuggestFilterValueGenerator(fieldName, fieldIds, values, false);
        } else {
            throw new ElasticsearchParseException("FieldConfig must be either [null], a string or a list of strings");
        }
    }

    /**
     * Parse a list of {@link FilteredSuggestCategoryQueryContext} using <code>parser</code>. A QueryContexts accepts
     * one of the following forms:
     *
     * <ul>
     * <li>Object: FilteredSuggestCategoryQueryContext</li>
     * <li>String: FilteredSuggestCategoryQueryContext value with boost=1</li>
     * <li>Array:
     * 
     * <pre>
     * [FilteredSuggestCategoryQueryContext, ..]
     * </pre>
     * 
     * </li>
     * </ul>
     *
     * A FilteredSuggestCategoryQueryContext has one of the following forms:
     * <ul>
     * <li>Object:
     * 
     * <pre>
     * {&quot;value&quot;: <i>&lt;string&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>}
     * </pre>
     * 
     * </li>
     * <li>String:
     * 
     * <pre>
     * &quot;string&quot;
     * </pre>
     * 
     * </li>
     * </ul>
     */
    @Override
    public List<InternalQueryContext> toInternalQueryContexts(List<FilteredSuggestCategoryQueryContext> queryContexts) {
        List<InternalQueryContext> internalInternalQueryContexts = new ArrayList<>(queryContexts.size());
        internalInternalQueryContexts.addAll(queryContexts.stream()
                .map(queryContext -> new InternalQueryContext(queryContext.getCategory(), queryContext.getBoost(), false))
                .collect(Collectors.toList()));

        return internalInternalQueryContexts;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FilteredSuggestCategoryFilterMapping) {
            FilteredSuggestCategoryFilterMapping other = (FilteredSuggestCategoryFilterMapping) obj;
            if (this.fieldName.equals(other.fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = fieldName.hashCode();
        hashCode = 31 * hashCode;

        return hashCode;
    }

    public static class Builder extends FilteredSuggestFilterBuilder<FilteredSuggestCategoryFilterMapping> {
        private String filterFieldPath;
        private String filterFieldIds;
        private boolean persist = true;
        private boolean indexFilterValue = false;

        public Builder(String name) {
            super(name);
        }

        /**
         * Set the path of the filter field to use
         */
        public Builder filterFieldPath(String filterFieldPath) {
            this.filterFieldPath = filterFieldPath;
            return this;
        }

        public Builder setFilterFieldIds(String filterFieldIds) {
            this.filterFieldIds = filterFieldIds;
            return this;
        }

        public Builder indexFilterValue(boolean indexFilterValue) {
            this.indexFilterValue = indexFilterValue;
            return this;
        }

        public Builder persist(boolean persist) {
            this.persist = persist;
            return this;
        }

        @Override
        public FilteredSuggestCategoryFilterMapping build() {
            return new FilteredSuggestCategoryFilterMapping(name, filterFieldPath, filterFieldIds, persist, indexFilterValue);
        }
    }
}
