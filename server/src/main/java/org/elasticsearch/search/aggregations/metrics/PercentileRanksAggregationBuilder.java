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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder.LeafOnly;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class PercentileRanksAggregationBuilder extends LeafOnly<ValuesSource, PercentileRanksAggregationBuilder> {
    public static final String NAME = PercentileRanks.TYPE_NAME;

    public static final ParseField VALUES_FIELD = new ParseField("values");

    private static class TDigestOptions {
        Double compression;
    }

    private static final ObjectParser<TDigestOptions, String> TDIGEST_OPTIONS_PARSER =
            new ObjectParser<>(PercentilesMethod.TDIGEST.getParseField().getPreferredName(), TDigestOptions::new);
    static {
        TDIGEST_OPTIONS_PARSER.declareDouble((opts, compression) -> opts.compression = compression, new ParseField("compression"));
    }

    private static class HDROptions {
        Integer numberOfSigDigits;
    }

    private static final ObjectParser<HDROptions, String> HDR_OPTIONS_PARSER =
            new ObjectParser<>(PercentilesMethod.HDR.getParseField().getPreferredName(), HDROptions::new);
    static {
        HDR_OPTIONS_PARSER.declareInt((opts, numberOfSigDigits) -> opts.numberOfSigDigits = numberOfSigDigits,
                new ParseField("number_of_significant_value_digits"));
    }

    // The builder requires two parameters for the constructor: aggregation name and values array.  The
    // agg name is supplied externally via the Parser's context (as a String), while the values array
    // is parsed from the request and supplied to the ConstructingObjectParser as a ctor argument
    private static final ConstructingObjectParser<PercentileRanksAggregationBuilder, String> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(PercentileRanksAggregationBuilder.NAME, false,
            (a, context) -> new PercentileRanksAggregationBuilder(context, (List) a[0]));
        ValuesSourceParserHelper.declareAnyFields(PARSER, true, true);
        PARSER.declareDoubleArray(constructorArg(), VALUES_FIELD);
        PARSER.declareBoolean(PercentileRanksAggregationBuilder::keyed, PercentilesAggregationBuilder.KEYED_FIELD);

        PARSER.declareField((b, v) -> {
            b.method(PercentilesMethod.TDIGEST);
            if (v.compression != null) {
                b.compression(v.compression);
            }
        }, TDIGEST_OPTIONS_PARSER::parse, PercentilesMethod.TDIGEST.getParseField(), ObjectParser.ValueType.OBJECT);

        PARSER.declareField((b, v) -> {
            b.method(PercentilesMethod.HDR);
            if (v.numberOfSigDigits != null) {
                b.numberOfSignificantValueDigits(v.numberOfSigDigits);
            }
        }, HDR_OPTIONS_PARSER::parse, PercentilesMethod.HDR.getParseField(), ObjectParser.ValueType.OBJECT);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        // the aggregation name is supplied to the parser as a Context. See note at top of Parser for more details
        return PARSER.parse(parser, aggregationName);
    }

    private double[] values;
    private PercentilesMethod method = PercentilesMethod.TDIGEST;
    private int numberOfSignificantValueDigits = 3;
    private double compression = 100.0;
    private boolean keyed = true;

    private PercentileRanksAggregationBuilder(String name, List<Double> values) {
        this(name, values.stream().mapToDouble(Double::doubleValue).toArray());
    }

    public PercentileRanksAggregationBuilder(String name, double[] values) {
        super(name, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        if (values == null) {
            throw new IllegalArgumentException("[values] must not be null: [" + name + "]");
        }
        if (values.length == 0) {
            throw new IllegalArgumentException("[values] must not be an empty array: [" + name + "]");
        }
        double[] sortedValues = Arrays.copyOf(values, values.length);
        Arrays.sort(sortedValues);
        this.values = sortedValues;
    }

    protected PercentileRanksAggregationBuilder(PercentileRanksAggregationBuilder clone,
                                                Builder factoriesBuilder,
                                                Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.values = clone.values;
        this.method = clone.method;
        this.numberOfSignificantValueDigits = clone.numberOfSignificantValueDigits;
        this.compression = clone.compression;
        this.keyed = clone.keyed;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new PercentileRanksAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public PercentileRanksAggregationBuilder(StreamInput in) throws IOException {
        super(in, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        values = in.readDoubleArray();
        keyed = in.readBoolean();
        numberOfSignificantValueDigits = in.readVInt();
        compression = in.readDouble();
        method = PercentilesMethod.readFromStream(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(values);
        out.writeBoolean(keyed);
        out.writeVInt(numberOfSignificantValueDigits);
        out.writeDouble(compression);
        method.writeTo(out);
    }

    /**
     * Get the values to compute percentiles from.
     */
    public double[] values() {
        return values;
    }

    /**
     * Set whether the XContent response should be keyed
     */
    public PercentileRanksAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /**
     * Get whether the XContent response should be keyed
     */
    public boolean keyed() {
        return keyed;
    }

    /**
     * Expert: set the number of significant digits in the values. Only relevant
     * when using {@link PercentilesMethod#HDR}.
     */
    public PercentileRanksAggregationBuilder numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
        if (numberOfSignificantValueDigits < 0 || numberOfSignificantValueDigits > 5) {
            throw new IllegalArgumentException("[numberOfSignificantValueDigits] must be between 0 and 5: [" + name + "]");
        }
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        return this;
    }

    /**
     * Expert: get the number of significant digits in the values. Only relevant
     * when using {@link PercentilesMethod#HDR}.
     */
    public int numberOfSignificantValueDigits() {
        return numberOfSignificantValueDigits;
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     */
    public PercentileRanksAggregationBuilder compression(double compression) {
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

    public PercentileRanksAggregationBuilder method(PercentilesMethod method) {
        if (method == null) {
            throw new IllegalArgumentException("[method] must not be null: [" + name + "]");
        }
        this.method = method;
        return this;
    }

    public PercentilesMethod method() {
        return method;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(QueryShardContext queryShardContext,
                                                                     ValuesSourceConfig<ValuesSource> config,
                                                                     AggregatorFactory parent,
                                                                     Builder subFactoriesBuilder) throws IOException {
        switch (method) {
        case TDIGEST:
            return new TDigestPercentileRanksAggregatorFactory(name, config, values, compression, keyed, queryShardContext, parent,
                    subFactoriesBuilder, metaData);
        case HDR:
            return new HDRPercentileRanksAggregatorFactory(name, config, values, numberOfSignificantValueDigits, keyed, queryShardContext,
                    parent, subFactoriesBuilder, metaData);
        default:
            throw new IllegalStateException("Illegal method [" + method + "]");
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.array(VALUES_FIELD.getPreferredName(), values);
        builder.field(PercentilesAggregationBuilder.KEYED_FIELD.getPreferredName(), keyed);
        builder.startObject(method.toString());
        if (method == PercentilesMethod.TDIGEST) {
            builder.field(PercentilesAggregationBuilder.COMPRESSION_FIELD.getPreferredName(), compression);
        } else {
            builder.field(PercentilesAggregationBuilder.NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        PercentileRanksAggregationBuilder other = (PercentileRanksAggregationBuilder) obj;
        if (Objects.equals(method, other.method) == false) {
            return false;
        }
        boolean equalSettings = false;
        switch (method) {
            case HDR:
                equalSettings = Objects.equals(numberOfSignificantValueDigits, other.numberOfSignificantValueDigits);
                break;
            case TDIGEST:
                equalSettings = Objects.equals(compression, other.compression);
                break;
            default:
                throw new IllegalStateException("Illegal method [" + method + "]");
        }
        return equalSettings
            && Objects.deepEquals(values, other.values)
            && Objects.equals(keyed, other.keyed)
            && Objects.equals(method, other.method);
    }

    @Override
    public int hashCode() {
        switch (method) {
            case HDR:
                return Objects.hash(super.hashCode(), Arrays.hashCode(values), keyed, numberOfSignificantValueDigits, method);
            case TDIGEST:
                return Objects.hash(super.hashCode(), Arrays.hashCode(values), keyed, compression, method);
            default:
                throw new IllegalStateException("Illegal method [" + method + "]");
        }
    }

    @Override
    public String getType() {
        return NAME;
    }
}
