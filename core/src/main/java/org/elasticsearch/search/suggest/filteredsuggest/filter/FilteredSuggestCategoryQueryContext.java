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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Defines the query context for {@link FilteredSuggestCategoryFilterMapping}
 */
public final class FilteredSuggestCategoryQueryContext implements ToXContent {
    public static final String NAME = "category";

    static final String CONTEXT_VALUE = "value";
    static final String CONTEXT_BOOST = "boost";

    private final String filterFieldValue;
    private final String fieldIds;
    private final int boost;

    private FilteredSuggestCategoryQueryContext(String filterFieldName, String fieldIds, int boost) {
        this.filterFieldValue = filterFieldName;
        this.fieldIds = fieldIds;
        this.boost = boost;
    }

    /**
     * Returns the category of the context
     */
    public String getCategory() {
        return fieldIds + filterFieldValue;
    }

    /**
     * The value of the category
     */
    public String getFilterFieldValue() {
        return filterFieldValue;
    }

    /**
     * Returns the fieldIds of each individual filter field. A field name can be created using multiple related fields
     * present in nested hierarchy.
     */
    public String getFieldIds() {
        return fieldIds;
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + boost;
        result = prime * result + ((fieldIds == null) ? 0 : fieldIds.hashCode());
        result = prime * result + ((filterFieldValue == null) ? 0 : filterFieldValue.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FilteredSuggestCategoryQueryContext other = (FilteredSuggestCategoryQueryContext) obj;
        if (boost != other.boost)
            return false;
        if (fieldIds == null) {
            if (other.fieldIds != null)
                return false;
        } else if (!fieldIds.equals(other.fieldIds))
            return false;
        if (filterFieldValue == null) {
            if (other.filterFieldValue != null)
                return false;
        } else if (!filterFieldValue.equals(other.filterFieldValue))
            return false;
        return true;
    }

    private static ObjectParser<Builder, Void> CATEGORY_PARSER = new ObjectParser<>(NAME, null);
    static {
        CATEGORY_PARSER.declareString(Builder::setFilterFieldValue, new ParseField(CONTEXT_VALUE));
        CATEGORY_PARSER.declareInt(Builder::setBoost, new ParseField(CONTEXT_BOOST));
    }

    public static FilteredSuggestCategoryQueryContext fromXContent(QueryParseContext context, String fieldIds) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();

        Builder builder = builder();

        if (token == XContentParser.Token.START_OBJECT) {
            CATEGORY_PARSER.parse(parser, builder, null);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            builder.setFilterFieldValue(parser.text());
        } else {
            throw new ElasticsearchParseException("category context must be an object or string");
        }

        builder.setFieldIds(fieldIds);

        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONTEXT_VALUE, filterFieldValue);
        builder.field(CONTEXT_BOOST, boost);
        builder.endObject();

        return builder;
    }

    public static class Builder {
        private String filterFieldValue;
        private String fieldIds;
        private int boost = 1;

        public Builder() {
        }

        /**
         * Sets the category of the category. This is a required field
         */
        public Builder setFilterFieldValue(String filterFieldValue) {
            Objects.requireNonNull(filterFieldValue, "Filter field value must not be null");
            this.filterFieldValue = filterFieldValue;
            return this;
        }

        /**
         * auto generated, cannot be null
         */
        public Builder setFieldIds(String fieldIds) {
            this.fieldIds = fieldIds;
            return this;
        }

        /**
         * Sets the query-time boost of the context. Defaults to 1.
         */
        public Builder setBoost(int boost) {
            if (boost <= 0) {
                throw new IllegalArgumentException("boost must be greater than 0");
            }
            this.boost = boost;
            return this;
        }

        public FilteredSuggestCategoryQueryContext build() {
            return new FilteredSuggestCategoryQueryContext(filterFieldValue, fieldIds, boost);
        }
    }
}
