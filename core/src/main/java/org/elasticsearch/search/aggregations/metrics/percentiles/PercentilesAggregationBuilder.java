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

package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.HDRPercentilesAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestPercentilesAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder.LeafOnly;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

public class PercentilesAggregationBuilder extends LeafOnly<ValuesSource.Numeric, PercentilesAggregationBuilder> {
    public static final String NAME = Percentiles.TYPE_NAME;

    public static final double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };
    public static final ParseField PERCENTS_FIELD = new ParseField("percents");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField METHOD_FIELD = new ParseField("method");
    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");
    public static final ParseField NUMBER_SIGNIFICANT_DIGITS_FIELD = new ParseField("number_of_significant_value_digits");

    private static class TDigestOptions {
        Double compression;
    }

    private static final ObjectParser<TDigestOptions, Void> TDIGEST_OPTIONS_PARSER =
            new ObjectParser<>(PercentilesMethod.TDIGEST.getParseField().getPreferredName(), TDigestOptions::new);
    static {
        TDIGEST_OPTIONS_PARSER.declareDouble((opts, compression) -> opts.compression = compression, COMPRESSION_FIELD);
    }

    private static class HDROptions {
        Integer numberOfSigDigits;
    }

    private static final ObjectParser<HDROptions, Void> HDR_OPTIONS_PARSER =
            new ObjectParser<>(PercentilesMethod.HDR.getParseField().getPreferredName(), HDROptions::new);
    static {
        HDR_OPTIONS_PARSER.declareInt(
                (opts, numberOfSigDigits) -> opts.numberOfSigDigits = numberOfSigDigits,
                NUMBER_SIGNIFICANT_DIGITS_FIELD);
    }

    private static final ObjectParser<InternalBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(PercentilesAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, false);

        PARSER.declareDoubleArray(
                (b, v) -> b.percentiles(v.stream().mapToDouble(Double::doubleValue).toArray()),
                PERCENTS_FIELD);

        PARSER.declareBoolean(PercentilesAggregationBuilder::keyed, KEYED_FIELD);

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
        InternalBuilder internal = PARSER.parse(parser, new InternalBuilder(aggregationName), null);
        // we need to return a PercentilesAggregationBuilder for equality checks to work
        PercentilesAggregationBuilder returnedAgg = new PercentilesAggregationBuilder(internal.name);
        setIfNotNull(returnedAgg::valueType, internal.valueType());
        setIfNotNull(returnedAgg::format, internal.format());
        setIfNotNull(returnedAgg::missing, internal.missing());
        setIfNotNull(returnedAgg::field, internal.field());
        setIfNotNull(returnedAgg::script, internal.script());
        setIfNotNull(returnedAgg::method, internal.method());
        setIfNotNull(returnedAgg::percentiles, internal.percentiles());
        returnedAgg.keyed(internal.keyed());
        returnedAgg.compression(internal.compression());
        returnedAgg.numberOfSignificantValueDigits(internal.numberOfSignificantValueDigits());
        return returnedAgg;
    }

    private static <T> void setIfNotNull(Consumer<T> consumer, T value) {
        if (value != null) {
            consumer.accept(value);
        }
    }

    private double[] percents = DEFAULT_PERCENTS;
    private PercentilesMethod method = PercentilesMethod.TDIGEST;
    private int numberOfSignificantValueDigits = 3;
    private double compression = 100.0;
    private boolean keyed = true;

    public PercentilesAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    /**
     * Read from a stream.
     */
    public PercentilesAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
        percents = in.readDoubleArray();
        keyed = in.readBoolean();
        numberOfSignificantValueDigits = in.readVInt();
        compression = in.readDouble();
        method = PercentilesMethod.readFromStream(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(percents);
        out.writeBoolean(keyed);
        out.writeVInt(numberOfSignificantValueDigits);
        out.writeDouble(compression);
        method.writeTo(out);
    }

    /**
     * Set the values to compute percentiles from.
     */
    public PercentilesAggregationBuilder percentiles(double... percents) {
        if (percents == null) {
            throw new IllegalArgumentException("[percents] must not be null: [" + name + "]");
        }
        if (percents.length == 0) {
            throw new IllegalArgumentException("[percents] must not be empty: [" + name + "]");
        }
        double[] sortedPercents = Arrays.copyOf(percents, percents.length);
        Arrays.sort(sortedPercents);
        this.percents = sortedPercents;
        return this;
    }

    /**
     * Get the values to compute percentiles from.
     */
    public double[] percentiles() {
        return percents;
    }

    /**
     * Set whether the XContent response should be keyed
     */
    public PercentilesAggregationBuilder keyed(boolean keyed) {
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
    public PercentilesAggregationBuilder numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
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
    public PercentilesAggregationBuilder compression(double compression) {
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

    public PercentilesAggregationBuilder method(PercentilesMethod method) {
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
    protected ValuesSourceAggregatorFactory<Numeric, ?> innerBuild(SearchContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        switch (method) {
        case TDIGEST:
            return new TDigestPercentilesAggregatorFactory(name, config, percents, compression, keyed, context, parent,
                    subFactoriesBuilder, metaData);
        case HDR:
            return new HDRPercentilesAggregatorFactory(name, config, percents, numberOfSignificantValueDigits, keyed, context, parent,
                    subFactoriesBuilder, metaData);
        default:
            throw new IllegalStateException("Illegal method [" + method + "]");
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.array(PERCENTS_FIELD.getPreferredName(), percents);
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        builder.startObject(method.toString());
        if (method == PercentilesMethod.TDIGEST) {
            builder.field(COMPRESSION_FIELD.getPreferredName(), compression);
        } else {
            builder.field(NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        PercentilesAggregationBuilder other = (PercentilesAggregationBuilder) obj;
        if (!Objects.equals(method, other.method)) {
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
            throw new IllegalStateException("Illegal method [" + method.toString() + "]");
        }
        return equalSettings
                && Objects.deepEquals(percents, other.percents)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(method, other.method);
    }

    @Override
    protected int innerHashCode() {
        switch (method) {
        case HDR:
            return Objects.hash(Arrays.hashCode(percents), keyed, numberOfSignificantValueDigits, method);
        case TDIGEST:
            return Objects.hash(Arrays.hashCode(percents), keyed, compression, method);
        default:
            throw new IllegalStateException("Illegal method [" + method.toString() + "]");
        }
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Private specialization of this builder that should only be used by the parser, this enables us to
     * overwrite {@link #method()} to check that it is not defined twice in xContent and throw
     * an error, while the Java API should allow to overwrite the method
     */
    private static class InternalBuilder extends PercentilesAggregationBuilder {

        private boolean setOnce = false;

        private InternalBuilder(String name) {
            super(name);
        }

        @Override
        public InternalBuilder method(PercentilesMethod method) {
            if (setOnce == false) {
                super.method(method);
                setOnce = true;
                return this;
            } else {
                throw new IllegalStateException("Only one percentiles method should be declared.");
            }
        }
    }
}
