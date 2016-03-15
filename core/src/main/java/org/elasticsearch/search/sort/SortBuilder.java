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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public abstract class SortBuilder<T extends SortBuilder<?>> implements ToXContent {

    protected static Nested resolveNested(QueryShardContext context, String nestedPath, QueryBuilder<?> nestedFilter) throws IOException {
        Nested nested = null;
        if (nestedPath != null) {
            BitSetProducer rootDocumentsFilter = context.bitsetFilter(Queries.newNonNestedFilter());
            ObjectMapper nestedObjectMapper = context.getObjectMapper(nestedPath);
            if (nestedObjectMapper == null) {
                throw new QueryShardException(context, "[nested] failed to find nested object under path [" + nestedPath + "]");
            }
            if (!nestedObjectMapper.nested().isNested()) {
                throw new QueryShardException(context, "[nested] nested object under path [" + nestedPath + "] is not of nested type");
            }
            Query innerDocumentsQuery;
            if (nestedFilter != null) {
                innerDocumentsQuery = nestedFilter.toFilter(context);
            } else {
                innerDocumentsQuery = nestedObjectMapper.nestedTypeFilter();
            }
            nested = new Nested(rootDocumentsFilter,  innerDocumentsQuery);
        }
        return nested;
    }

    protected SortOrder order = SortOrder.ASC;
    public static final ParseField ORDER_FIELD = new ParseField("order");

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to build query", e);
        }
    }

    /**
     * Set the order of sorting.
     */
    @SuppressWarnings("unchecked")
    public T order(SortOrder order) {
        Objects.requireNonNull(order, "sort order cannot be null.");
        this.order = order;
        return (T) this;
    }

    /**
     * Return the {@link SortOrder} used for this {@link SortBuilder}.
     */
    public SortOrder order() {
        return this.order;
    }
}
