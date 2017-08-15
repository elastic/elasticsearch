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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

public class AutoDateHistogramAggregationBuilder
        extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, AutoDateHistogramAggregationBuilder> {

    public static final String NAME = "auto_date_histogram";

    public static final ParseField NUM_BUCKETS_FIELD = new ParseField("buckets");

    private static final ObjectParser<AutoDateHistogramAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(AutoDateHistogramAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, true);

        PARSER.declareInt(AutoDateHistogramAggregationBuilder::setNumBuckets, NUM_BUCKETS_FIELD);
    }

    public static AutoDateHistogramAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new AutoDateHistogramAggregationBuilder(aggregationName), null);
    }

    private int numBuckets = 10;

    /** Create a new builder with the given name. */
    public AutoDateHistogramAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.DATE);
    }

    /** Read from a stream, for internal use only. */
    public AutoDateHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.NUMERIC, ValueType.DATE);
        numBuckets = in.readVInt();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(numBuckets);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public AutoDateHistogramAggregationBuilder setNumBuckets(int numBuckets) {
        if (numBuckets <= 0) {
            throw new IllegalArgumentException(NUM_BUCKETS_FIELD.getPreferredName() + " must be greater than 0 for [" + name + "]");
        }
        this.numBuckets = numBuckets;
        return this;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    @Override
    protected ValuesSourceAggregatorFactory<Numeric, ?> innerBuild(SearchContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        Rounding[] roundings = new Rounding[6];
        roundings[0] = createRounding(DateTimeUnit.SECOND_OF_MINUTE);
        roundings[1] = createRounding(DateTimeUnit.MINUTES_OF_HOUR);
        roundings[2] = createRounding(DateTimeUnit.HOUR_OF_DAY);
        roundings[3] = createRounding(DateTimeUnit.DAY_OF_MONTH);
        roundings[4] = createRounding(DateTimeUnit.MONTH_OF_YEAR);
        roundings[5] = createRounding(DateTimeUnit.YEAR_OF_CENTURY);
        return new AutoDateHistogramAggregatorFactory(name, config, numBuckets, roundings, context, parent, subFactoriesBuilder, metaData);
    }

    private Rounding createRounding(DateTimeUnit interval) {
        Rounding.Builder tzRoundingBuilder = Rounding.builder(interval);
        if (timeZone() != null) {
            tzRoundingBuilder.timeZone(timeZone());
        }
        Rounding rounding = tzRoundingBuilder.build();
        return rounding;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(NUM_BUCKETS_FIELD.getPreferredName(), numBuckets);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(numBuckets);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        AutoDateHistogramAggregationBuilder other = (AutoDateHistogramAggregationBuilder) obj;
        return Objects.equals(numBuckets, other.numBuckets);
    }
}
