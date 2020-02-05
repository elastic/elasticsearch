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

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * This provides a base class for aggregations that are building percentiles or percentiles-like functionality (e.g. percentile ranks).
 * It provides a set of common fields/functionality for setting the available algorithms (TDigest and HDRHistogram),
 * as well as algorithm-specific settings via a {@link PercentilesMethod.Config} object
 */
public abstract class AbstractPercentilesAggregationBuilder<T extends ValuesSourceAggregationBuilder<ValuesSource, T>>
    extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, T> {

    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    protected boolean keyed = true;
    protected double[] values;
    private PercentilesMethod.Config percentilesConfig;
    private ParseField valuesField;

    AbstractPercentilesAggregationBuilder(String name, double[] values, PercentilesMethod.Config percentilesConfig,
                                          ParseField valuesField) {
        super(name, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        if (values == null) {
            throw new IllegalArgumentException("[" + valuesField.getPreferredName() + "] must not be null: [" + name + "]");
        }
        if (values.length == 0) {
            throw new IllegalArgumentException("[" + valuesField.getPreferredName() + "] must not be an empty array: [" + name + "]");
        }
        double[] sortedValues = Arrays.copyOf(values, values.length);
        Arrays.sort(sortedValues);
        this.values = sortedValues;
        this.percentilesConfig = percentilesConfig;
        this.valuesField = valuesField;
    }

    AbstractPercentilesAggregationBuilder(String name, ParseField valuesField) {
        super(name,  CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        this.valuesField = valuesField;
    }

    AbstractPercentilesAggregationBuilder(AbstractPercentilesAggregationBuilder<T> clone,
                                          AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.percentilesConfig = clone.percentilesConfig;
        this.keyed = clone.keyed;
        this.values = clone.values;
        this.valuesField = clone.valuesField;
    }

    AbstractPercentilesAggregationBuilder(StreamInput in) throws IOException {
        super(in, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        values = in.readDoubleArray();
        keyed = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            percentilesConfig
                = (PercentilesMethod.Config) in.readOptionalWriteable((Reader<Writeable>) PercentilesMethod.Config::fromStream);
        } else {
            int numberOfSignificantValueDigits = in.readVInt();
            double compression = in.readDouble();
            PercentilesMethod method = PercentilesMethod.readFromStream(in);
            percentilesConfig = PercentilesMethod.Config.fromLegacy(method, compression, numberOfSignificantValueDigits);
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(values);
        out.writeBoolean(keyed);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeOptionalWriteable(percentilesConfig);
        } else {
            // Legacy method serialized both SigFigs and compression, even though we only need one.  So we need
            // to serialize the default for the unused method
            int numberOfSignificantValueDigits = percentilesConfig.getMethod().equals(PercentilesMethod.HDR)
                ? ((PercentilesMethod.Config.Hdr)percentilesConfig).getNumberOfSignificantValueDigits()
                : PercentilesMethod.Config.Hdr.DEFAULT_NUMBER_SIG_FIGS;

            double compression = percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)
                ? ((PercentilesMethod.Config.TDigest)percentilesConfig).getCompression()
                : PercentilesMethod.Config.TDigest.DEFAULT_COMPRESSION;

            out.writeVInt(numberOfSignificantValueDigits);
            out.writeDouble(compression);
            percentilesConfig.getMethod().writeTo(out);
        }
    }

    /**
     * Set whether the XContent response should be keyed
     */
    public T keyed(boolean keyed) {
        this.keyed = keyed;
        return (T) this;
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
     *
     * Deprecated: set numberOfSignificantValueDigits by configuring a {@link PercentilesMethod.Config.Hdr} instead
     * and set via {@link PercentilesAggregationBuilder#setPercentilesConfig(PercentilesMethod.Config)}
     */
    @Deprecated
    public T numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
        if (numberOfSignificantValueDigits < 0 || numberOfSignificantValueDigits > 5) {
            throw new IllegalArgumentException("[numberOfSignificantValueDigits] must be between 0 and 5: [" + name + "]");
        }

        if (percentilesConfig == null || percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
            percentilesConfig = new PercentilesMethod.Config.Hdr(numberOfSignificantValueDigits);
        } else {
            throw new IllegalArgumentException("Cannot set [numberOfSignificantValueDigits] because the method " +
                "has already been configured for TDigest");
        }

        return (T) this;
    }

    /**
     * Expert: get the number of significant digits in the values. Only relevant
     * when using {@link PercentilesMethod#HDR}.
     *
     * Deprecated: get numberOfSignificantValueDigits by inspecting the {@link PercentilesMethod.Config} returned from
     * {@link PercentilesAggregationBuilder#getPercentilesConfig()} instead
     */
    @Deprecated
    public int numberOfSignificantValueDigits() {
        if (percentilesConfig != null && percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
            return ((PercentilesMethod.Config.Hdr)percentilesConfig).getNumberOfSignificantValueDigits();
        }
        throw new IllegalStateException("Percentiles [method] has not been configured yet, or is a TDigest");
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     *
     * Deprecated: set compression by configuring a {@link PercentilesMethod.Config.TDigest} instead
     * and set via {@link PercentilesAggregationBuilder#setPercentilesConfig(PercentilesMethod.Config)}
     */
    @Deprecated
    public T compression(double compression) {
        if (compression < 0.0) {
            throw new IllegalArgumentException(
                "[compression] must be greater than or equal to 0. Found [" + compression + "] in [" + name + "]");
        }

        if (percentilesConfig == null || percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
            percentilesConfig = new PercentilesMethod.Config.TDigest(compression);
        } else {
            throw new IllegalArgumentException("Cannot set [compression] because the method has already been configured for HDRHistogram");
        }
        return (T) this;
    }

    /**
     * Expert: get the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     *
     * Deprecated: get compression by inspecting the {@link PercentilesMethod.Config} returned from
     * {@link PercentilesAggregationBuilder#getPercentilesConfig()} instead
     */
    @Deprecated
    public double compression() {
        if (percentilesConfig != null && percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
            return ((PercentilesMethod.Config.TDigest)percentilesConfig).getCompression();
        }
        throw new IllegalStateException("Percentiles [method] has not been configured yet, or is a HdrHistogram");
    }


    /**
     * Deprecated: set method by configuring a {@link PercentilesMethod.Config} instead
     * and set via {@link PercentilesAggregationBuilder#setPercentilesConfig(PercentilesMethod.Config)}
     */
    @Deprecated
    public T method(PercentilesMethod method) {
        if (method == null) {
            throw new IllegalArgumentException("[method] must not be null: [" + name + "]");
        }
        if (percentilesConfig == null) {
            if (method.equals(PercentilesMethod.TDIGEST) ) {
                this.percentilesConfig = new PercentilesMethod.Config.TDigest();
            } else {
                this.percentilesConfig = new PercentilesMethod.Config.Hdr();
            }
        } else if (percentilesConfig.getMethod().equals(method) == false) {
            // we already have an algo configured, but it's different from the requested method
            // reset to default for the requested method
            if (method.equals(PercentilesMethod.TDIGEST) ) {
                this.percentilesConfig = new PercentilesMethod.Config.TDigest();
            } else {
                this.percentilesConfig = new PercentilesMethod.Config.Hdr();
            }
        } // if method and config were same, this is a no-op so we don't overwrite settings

        return (T) this;
    }

    /**
     * Deprecated: get method by inspecting the {@link PercentilesMethod.Config} returned from
     * {@link PercentilesAggregationBuilder#getPercentilesConfig()} instead
     */
    @Nullable
    @Deprecated
    public PercentilesMethod method() {
        return percentilesConfig == null ? null : percentilesConfig.getMethod();
    }

    /**
     * Returns how the percentiles algorithm has been configured, or null if it has not been configured yet
     */
    @Nullable
    public PercentilesMethod.Config getPercentilesConfig() {
        return percentilesConfig;
    }

    /**
     * Sets how the percentiles algorithm should be configured
     */
    public T setPercentilesConfig(PercentilesMethod.Config percentilesConfig) {
        this.percentilesConfig = percentilesConfig;
        return (T) this;
    }

    /**
     * Return the current algo configuration, or a default (Tdigest) otherwise
     *
     * This is needed because builders don't have a "build" or "finalize" method, but
     * the old API did bake in defaults.  Certain operations like xcontent, equals, hashcode
     * will use the values in the builder at any time and need to be aware of defaults.
     *
     * But to maintain BWC behavior as much as possible, we allow the user to set
     * algo settings independent of method.  To keep life simple we use a null to track
     * if any method has been selected yet.
     *
     * However, this means we need a way to fetch the default if the user hasn't
     * selected any method and uses a builder-side feature like xcontent
     */
    PercentilesMethod.Config configOrDefault() {
        if (percentilesConfig == null) {
            return new PercentilesMethod.Config.TDigest();
        }
        return percentilesConfig;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.array(valuesField.getPreferredName(), values);
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        builder = configOrDefault().toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        AbstractPercentilesAggregationBuilder other = (AbstractPercentilesAggregationBuilder) obj;
        return Objects.deepEquals(values, other.values)
            && Objects.equals(keyed, other.keyed)
            && Objects.equals(configOrDefault(), other.configOrDefault());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(values), keyed, configOrDefault());
    }
}
