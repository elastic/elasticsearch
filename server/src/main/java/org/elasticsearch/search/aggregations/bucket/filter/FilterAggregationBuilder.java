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

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class FilterAggregationBuilder extends AbstractAggregationBuilder<FilterAggregationBuilder> {
    public static final String NAME = "filter";

    private final QueryBuilder filter;

    /**
     * @param name
     *            the name of this aggregation
     * @param filter
     *            Set the filter to use, only documents that match this
     *            filter will fall into the bucket defined by this
     *            {@link Filter} aggregation.
     */
    public FilterAggregationBuilder(String name, QueryBuilder filter) {
        super(name);
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null: [" + name + "]");
        }
        this.filter = filter;
    }

    protected FilterAggregationBuilder(FilterAggregationBuilder clone,
                                       AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.filter = clone.filter;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new FilterAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public FilterAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        filter = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(filter);
    }

    @Override
    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder result = Rewriteable.rewrite(filter, queryShardContext);
        if (result != filter) {
            return new FilterAggregationBuilder(getName(), result);
        }
        return this;
    }

    @Override
    protected AggregatorFactory doBuild(SearchContext context, AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new FilterAggregatorFactory(name, filter, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter != null) {
            filter.toXContent(builder, params);
        }
        return builder;
    }

    public static FilterAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        QueryBuilder filter = parseInnerQueryBuilder(parser);
        return new FilterAggregationBuilder(aggregationName, filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        FilterAggregationBuilder other = (FilterAggregationBuilder) obj;
        return Objects.equals(filter, other.filter);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public QueryBuilder getFilter() {
        return filter;
    }
}
