/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.SearchPlugin.AggregationSpec;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.elasticsearch.search.aggregations.metrics.PercentilesMethod.COMPRESSION_FIELD;
import static org.elasticsearch.search.aggregations.support.CoreValuesSourceType.NUMERIC;
import static org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType.HISTOGRAM;

public class BoxplotAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, BoxplotAggregationBuilder> {
    public static final String NAME = "boxplot";
    static final ObjectParser<BoxplotAggregationBuilder, String> PARSER =
        ObjectParser.fromBuilder(NAME, BoxplotAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(BoxplotAggregationBuilder::compression, COMPRESSION_FIELD);
    }

    public static AggregationSpec spec(UnaryOperator<ContextParser<String, BoxplotAggregationBuilder>> parserWrapper) {
        return new AggregationSpec(NAME, BoxplotAggregationBuilder::new, parserWrapper.apply(PARSER))
            .addResultReader(InternalBoxplot::new)
            .implementFor((BoxplotAggregatorSupplier) BoxplotAggregator::new, NUMERIC, HISTOGRAM);
    }

    private double compression = 100.0;

    public BoxplotAggregationBuilder(String name) {
        super(name);
    }

    protected BoxplotAggregationBuilder(BoxplotAggregationBuilder clone,
                                        AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.compression = clone.compression;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new BoxplotAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    BoxplotAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        compression = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(compression);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return NUMERIC;
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     */
    public BoxplotAggregationBuilder compression(double compression) {
        if (compression < 0.0) {
            throw new IllegalArgumentException(
                "[compression] must be greater than or equal to 0. Found [" + compression + "] in [" + name + "]");
        }
        this.compression = compression;
        return this;
    }

    /**
     * Expert: get the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     */
    public double compression() {
        return compression;
    }

    @Override
    protected BoxplotAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                  ValuesSourceConfig config,
                                                  AggregatorFactory parent,
                                                  AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new BoxplotAggregatorFactory(name, config, compression, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(COMPRESSION_FIELD.getPreferredName(), compression);
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BoxplotAggregationBuilder other = (BoxplotAggregationBuilder) obj;
        return Objects.equals(compression, other.compression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), compression);
    }

    @Override
    public String getType() {
        return NAME;
    }
}

