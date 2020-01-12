/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class StringStatsAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.Bytes, StringStatsAggregationBuilder> {

    public static final String NAME = "string_stats";
    private boolean showDistribution = false;

    private static final ObjectParser<StringStatsAggregationBuilder, Void> PARSER;
    private static final ParseField SHOW_DISTRIBUTION_FIELD = new ParseField("show_distribution");

    static {
        PARSER = new ObjectParser<>(StringStatsAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareBytesFields(PARSER, true, true);

        PARSER.declareBoolean(StringStatsAggregationBuilder::showDistribution, StringStatsAggregationBuilder.SHOW_DISTRIBUTION_FIELD);
    }

    public static StringStatsAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new StringStatsAggregationBuilder(aggregationName), null);
    }

    public StringStatsAggregationBuilder(String name) {
        super(name, CoreValuesSourceType.BYTES, ValueType.STRING);
    }

    public StringStatsAggregationBuilder(StringStatsAggregationBuilder clone,
                                         AggregatorFactories.Builder factoriesBuilder,
                                         Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.showDistribution = clone.showDistribution();
    }

    /** Read from a stream. */
    public StringStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in, CoreValuesSourceType.BYTES, ValueType.STRING);
        this.showDistribution = in.readBoolean();
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new StringStatsAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(showDistribution);
    }

    @Override
    protected StringStatsAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                      ValuesSourceConfig<ValuesSource.Bytes> config,
                                                      AggregatorFactory parent,
                                                      AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new StringStatsAggregatorFactory(name, config, showDistribution, queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(StringStatsAggregationBuilder.SHOW_DISTRIBUTION_FIELD.getPreferredName(), showDistribution);

        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Return whether to include the probability distribution of each character in the results.
     * {@code showDistribution} is true, distribution will be included.
     */
    public boolean showDistribution() {
        return showDistribution;
    }

    /**
     * Set whether to include the probability distribution of each character in the results.
     *
     * @return the builder so that calls can be chained
     */
    public StringStatsAggregationBuilder showDistribution(boolean showDistribution) {
        this.showDistribution = showDistribution;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), showDistribution);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        StringStatsAggregationBuilder other = (StringStatsAggregationBuilder) obj;
        return showDistribution == other.showDistribution;
    }
}
