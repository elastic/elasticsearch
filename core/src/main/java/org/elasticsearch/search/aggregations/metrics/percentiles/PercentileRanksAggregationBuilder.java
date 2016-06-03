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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.HDRPercentileRanksAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestPercentileRanksAggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder.LeafOnly;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class PercentileRanksAggregationBuilder extends LeafOnly<ValuesSource.Numeric, PercentileRanksAggregationBuilder> {
    public static final String NAME = InternalTDigestPercentileRanks.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private double[] values;
    private PercentilesMethod method = PercentilesMethod.TDIGEST;
    private int numberOfSignificantValueDigits = 3;
    private double compression = 100.0;
    private boolean keyed = true;

    public PercentileRanksAggregationBuilder(String name) {
        super(name, InternalTDigestPercentileRanks.TYPE, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    /**
     * Read from a stream.
     */
    public PercentileRanksAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalTDigestPercentileRanks.TYPE, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
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
     * Set the values to compute percentiles from.
     */
    public PercentileRanksAggregationBuilder values(double... values) {
        if (values == null) {
            throw new IllegalArgumentException("[values] must not be null: [" + name + "]");
        }
        double[] sortedValues = Arrays.copyOf(values, values.length);
        Arrays.sort(sortedValues);
        this.values = sortedValues;
        return this;
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
    protected ValuesSourceAggregatorFactory<Numeric, ?> innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        switch (method) {
        case TDIGEST:
            return new TDigestPercentileRanksAggregatorFactory(name, type, config, values, compression, keyed, context, parent,
                    subFactoriesBuilder, metaData);
        case HDR:
            return new HDRPercentileRanksAggregatorFactory(name, type, config, values, numberOfSignificantValueDigits, keyed, context,
                    parent, subFactoriesBuilder, metaData);
        default:
            throw new IllegalStateException("Illegal method [" + method.getName() + "]");
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(PercentileRanksParser.VALUES_FIELD.getPreferredName(), values);
        builder.field(AbstractPercentilesParser.KEYED_FIELD.getPreferredName(), keyed);
        builder.startObject(method.getName());
        if (method == PercentilesMethod.TDIGEST) {
            builder.field(AbstractPercentilesParser.COMPRESSION_FIELD.getPreferredName(), compression);
        } else {
            builder.field(AbstractPercentilesParser.NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        PercentileRanksAggregationBuilder other = (PercentileRanksAggregationBuilder) obj;
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
            throw new IllegalStateException("Illegal method [" + method.getName() + "]");
        }
        return equalSettings
                && Objects.deepEquals(values, other.values)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(method, other.method);
    }

    @Override
    protected int innerHashCode() {
        switch (method) {
        case HDR:
            return Objects.hash(Arrays.hashCode(values), keyed, numberOfSignificantValueDigits, method);
        case TDIGEST:
            return Objects.hash(Arrays.hashCode(values), keyed, compression, method);
        default:
            throw new IllegalStateException("Illegal method [" + method.getName() + "]");
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
