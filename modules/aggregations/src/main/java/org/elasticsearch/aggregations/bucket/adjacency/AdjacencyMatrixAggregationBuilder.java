/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.adjacency;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.aggregations.bucket.adjacency.AdjacencyMatrixAggregator.KeyedFilter;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class AdjacencyMatrixAggregationBuilder extends AbstractAggregationBuilder<AdjacencyMatrixAggregationBuilder> {
    public static final String NAME = "adjacency_matrix";

    private static final String DEFAULT_SEPARATOR = "&";

    private static final ParseField SEPARATOR_FIELD = new ParseField("separator");
    private static final ParseField FILTERS_FIELD = new ParseField("filters");
    private List<KeyedFilter> filters;
    private String separator = DEFAULT_SEPARATOR;

    private static final ObjectParser<AdjacencyMatrixAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        AdjacencyMatrixAggregationBuilder::new
    );
    static {
        PARSER.declareString(AdjacencyMatrixAggregationBuilder::separator, SEPARATOR_FIELD);
        PARSER.declareNamedObjects(AdjacencyMatrixAggregationBuilder::setFiltersAsList, KeyedFilter.PARSER, FILTERS_FIELD);
    }

    public static AggregationBuilder parse(XContentParser parser, String name) throws IOException {
        AdjacencyMatrixAggregationBuilder result = PARSER.parse(parser, name);
        result.checkConsistency();
        return result;
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

    protected AdjacencyMatrixAggregationBuilder(
        AdjacencyMatrixAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.filters = new ArrayList<>(clone.filters);
        this.separator = clone.separator;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new AdjacencyMatrixAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public boolean supportsSampling() {
        return true;
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
        filters = new ArrayList<>(filtersSize);
        for (int i = 0; i < filtersSize; i++) {
            filters.add(new KeyedFilter(in));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(filters.size());
        out.writeString(separator);
        for (KeyedFilter keyedFilter : filters) {
            keyedFilter.writeTo(out);
        }
    }

    private void checkConsistency() {
        if ((filters == null) || (filters.size() == 0)) {
            throw new IllegalStateException("[" + name + "] is missing : " + FILTERS_FIELD.getPreferredName() + " parameter");
        }
    }

    private void setFiltersAsMap(Map<String, QueryBuilder> filters) {
        // Convert uniquely named objects into internal KeyedFilters
        this.filters = new ArrayList<>(filters.size());
        for (Entry<String, QueryBuilder> kv : filters.entrySet()) {
            this.filters.add(new KeyedFilter(kv.getKey(), kv.getValue()));
        }
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        Collections.sort(this.filters, Comparator.comparing(KeyedFilter::key));
    }

    private AdjacencyMatrixAggregationBuilder setFiltersAsList(List<KeyedFilter> filters) {
        this.filters = new ArrayList<>(filters);
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        Collections.sort(this.filters, Comparator.comparing(KeyedFilter::key));
        return this;
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
     * Get the filters. This will be an unmodifiable map
     */
    public Map<String, QueryBuilder> filters() {
        Map<String, QueryBuilder> result = Maps.newMapWithExpectedSize(this.filters.size());
        for (KeyedFilter keyedFilter : this.filters) {
            result.put(keyedFilter.key(), keyedFilter.filter());
        }
        return result;
    }

    @Override
    protected AdjacencyMatrixAggregationBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean modified = false;
        List<KeyedFilter> rewrittenFilters = new ArrayList<>(filters.size());
        for (KeyedFilter kf : filters) {
            QueryBuilder rewritten = Rewriteable.rewrite(kf.filter(), queryRewriteContext);
            modified = modified || rewritten != kf.filter();
            rewrittenFilters.add(new KeyedFilter(kf.key(), rewritten));
        }
        if (modified) {
            return new AdjacencyMatrixAggregationBuilder(name).separator(separator).setFiltersAsList(rewrittenFilters);
        }
        return this;
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        int maxFilters = IndexSearcher.getMaxClauseCount();
        if (filters.size() > maxFilters) {
            throw new IllegalArgumentException(
                "Number of filters is too large, must be less than or equal to: ["
                    + maxFilters
                    + "] but was ["
                    + filters.size()
                    + "].  "
                    + "You can increase this limit by scaling up your java heap"
            );
        }
        return new AdjacencyMatrixAggregatorFactory(name, filters, separator, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SEPARATOR_FIELD.getPreferredName(), separator);
        builder.startObject(AdjacencyMatrixAggregator.FILTERS_FIELD.getPreferredName());
        for (KeyedFilter keyedFilter : filters) {
            builder.field(keyedFilter.key(), keyedFilter.filter());
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filters, separator);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        AdjacencyMatrixAggregationBuilder other = (AdjacencyMatrixAggregationBuilder) obj;
        return Objects.equals(filters, other.filters) && Objects.equals(separator, other.separator);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
