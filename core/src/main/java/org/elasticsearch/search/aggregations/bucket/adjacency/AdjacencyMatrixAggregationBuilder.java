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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregator.KeyedFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class AdjacencyMatrixAggregationBuilder extends AbstractAggregationBuilder<AdjacencyMatrixAggregationBuilder> {
    public static final String NAME = "adjacency_matrix";

    private static final String DEFAULT_SEPARATOR = "&";
    private static final int DEFAULT_MIN_DOC_COUNT = 1;

    private static final ParseField SEPARATOR_FIELD = new ParseField("separator");
    private static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");
    private static final ParseField FILTERS_FIELD = new ParseField("filters");
    private List<KeyedFilter> filters;
    private String separator = DEFAULT_SEPARATOR;
    private int minDocCount = DEFAULT_MIN_DOC_COUNT;

    public static Aggregator.Parser getParser() {
        ObjectParser<AdjacencyMatrixAggregationBuilder, QueryParseContext> parser = new ObjectParser<>(
                AdjacencyMatrixAggregationBuilder.NAME);
        parser.declareInt(AdjacencyMatrixAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD);
        parser.declareString(AdjacencyMatrixAggregationBuilder::separator, SEPARATOR_FIELD);
        parser.declareNamedObjects(AdjacencyMatrixAggregationBuilder::setFiltersAsList, KeyedFilter.PARSER, FILTERS_FIELD);
        return new Aggregator.Parser() {
            @Override
            public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
                AdjacencyMatrixAggregationBuilder result = parser.parse(context.parser(),
                        new AdjacencyMatrixAggregationBuilder(aggregationName), context);
                result.checkConsistency();
                return result;
            }
        };
    }

    protected void checkConsistency() {
        if ((filters == null) || (filters.size() == 0)) {
            throw new IllegalStateException("[" + name  + "] is missing : " + FILTERS_FIELD.getPreferredName() + " parameter");
        }        
    }


    protected void setFiltersAsMap(Map<String, QueryBuilder> filters) {
        // Convert uniquely named objects into internal KeyedFilters
        this.filters = new ArrayList<>(filters.size());
        for (Entry<String, QueryBuilder> kv : filters.entrySet()) {
            this.filters.add(new KeyedFilter(kv.getKey(), kv.getValue()));
        }
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        Collections.sort(this.filters, Comparator.comparing(KeyedFilter::key));
    }

    protected void setFiltersAsList(List<KeyedFilter> filters) {
        this.filters = new ArrayList<>(filters);
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        Collections.sort(this.filters, Comparator.comparing(KeyedFilter::key));
    }
    
   
    /**
     * @param name
     *            the name of this aggregation
     */
    protected AdjacencyMatrixAggregationBuilder(String name) {
        super(name);
    }    
    
    
    /**
     * @param name
     *            the name of this aggregation
     * @param filters
     *            the filters and their keys to use with this aggregation.
     */
    public AdjacencyMatrixAggregationBuilder(String name, Map<String, QueryBuilder> filters) {
        this(name, DEFAULT_SEPARATOR, filters);
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param separator
     *            the string used to separate keys in intersections buckets e.g.
     *            &amp; character for keyed filters A and B would return an
     *            intersection bucket named A&amp;B
     * @param filters
     *            the filters and their key to use with this aggregation.
     */
    public AdjacencyMatrixAggregationBuilder(String name, String separator, Map<String, QueryBuilder> filters) {
        super(name);
        this.separator = separator;
        setFiltersAsMap(filters);
    }

    /**
     * Read from a stream.
     */
    public AdjacencyMatrixAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        int filtersSize = in.readVInt();
        separator = in.readString();
        minDocCount = in.readVInt();
        filters = new ArrayList<>(filtersSize);
        for (int i = 0; i < filtersSize; i++) {
            filters.add(new KeyedFilter(in));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(filters.size());
        out.writeString(separator);
        out.writeVInt(minDocCount);
        for (KeyedFilter keyedFilter : filters) {
            keyedFilter.writeTo(out);
        }
    }

    /**
     * Set the separator used to join pairs of bucket keys
     */
    public AdjacencyMatrixAggregationBuilder separator(String separator) {
        if (separator == null) {
            throw new IllegalArgumentException("[separator] must not be null: [" + name + "]");
        }
        this.separator = separator;
        return this;
    }

    /**
     * Get the separator used to join pairs of bucket keys
     */
    public String separator() {
        return separator;
    }    
    
    
    /**
     * Set the miniumum number of docs a bucket should contain before it is returned
     */
    public AdjacencyMatrixAggregationBuilder minDocCount(int minDocCount) {
        if (minDocCount <= 0) {
            throw new IllegalArgumentException("["+MIN_DOC_COUNT_FIELD.getPreferredName()
                +"] must be greater than 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;        
        return this;
    }

    /**
     * Get the miniumum number of docs a bucket should contain before it is returned
     */
    public int minDocCount() {
        return minDocCount;
    }
    
    /**
     * Get the filters. This will be an unmodifiable map
     */
    public Map<String, QueryBuilder> filters() {
        Map<String, QueryBuilder>result = new HashMap<>(this.filters.size());
        for (KeyedFilter keyedFilter : this.filters) {
            result.put(keyedFilter.key(), keyedFilter.filter());
        }
        return result;
    }    
    

    @Override
    protected AggregatorFactory<?> doBuild(SearchContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
            throws IOException {
        List<KeyedFilter> rewrittenFilters = new ArrayList<>();
        for (KeyedFilter kf : filters) {
            rewrittenFilters.add(new KeyedFilter(kf.key(), QueryBuilder.rewriteQuery(kf.filter(), context.getQueryShardContext())));
        }
        return new AdjacencyMatrixAggregatorFactory(name, rewrittenFilters, separator, minDocCount, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SEPARATOR_FIELD.getPreferredName(), separator);
        builder.field(MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);
        builder.startObject(AdjacencyMatrixAggregator.FILTERS_FIELD.getPreferredName());
        for (KeyedFilter keyedFilter : filters) {
            builder.field(keyedFilter.key(), keyedFilter.filter());
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filters, separator, minDocCount);
    }

    @Override
    protected boolean doEquals(Object obj) {
        AdjacencyMatrixAggregationBuilder other = (AdjacencyMatrixAggregationBuilder) obj;
        return Objects.equals(filters, other.filters) && Objects.equals(separator, other.separator)
                && minDocCount == other.minDocCount;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
