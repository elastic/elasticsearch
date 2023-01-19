/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.Version;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This provides a base class for aggregations that are building percentiles or percentiles-like functionality (e.g. percentile ranks).
 * It provides a set of common fields/functionality for setting the available algorithms (TDigest and HDRHistogram),
 * as well as algorithm-specific settings via a {@link PercentilesConfig} object
 */
public abstract class AbstractPercentilesAggregationBuilder<T extends AbstractPercentilesAggregationBuilder<T>> extends
    ValuesSourceAggregationBuilder.MetricsAggregationBuilder<T> {

    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    private final ParseField valuesField;
    protected boolean keyed = true;
    protected double[] values;
    private PercentilesConfig percentilesConfig;

    public static <T extends AbstractPercentilesAggregationBuilder<T>> ConstructingObjectParser<T, String> createParser(
        String aggName,
        TriFunction<String, double[], PercentilesConfig, T> ctor,
        Supplier<PercentilesConfig> defaultConfig,
        ParseField valuesField
    ) {

        /**
         * This is a non-ideal ConstructingObjectParser, because it is a compromise between Percentiles and Ranks.
         * Ranks requires an array of values because there is no sane default, and we want to keep that in the ctor.
         * Percentiles has defaults, which means the API allows the user to either use the default or configure
         * their own.
         *
         * The mutability of Percentiles keeps us from having a strict ConstructingObjectParser, while the ctor
         * of Ranks keeps us from using a regular ObjectParser.
         *
         * This is a compromise, in that it is a ConstructingOP which accepts all optional arguments, and then we sort
         * out the behavior from there
         *
         * `args` are provided from the ConstructingObjectParser in-order they are defined in the parser.  So:
         *  - args[0]: values
         *  - args[1]: tdigest config options
         *  - args[2]: hdr config options
         *
         *  If `args` is null or empty, it means all were omitted.  This is usually an anti-pattern for
         *  ConstructingObjectParser, but we're allowing it because of the above-mentioned reasons
         */
        ConstructingObjectParser<T, String> parser = new ConstructingObjectParser<>(aggName, false, (args, name) -> {

            if (args == null || args.length == 0) {
                // Note: if this is a Percentiles agg, the null `values` will be converted into a default,
                // whereas a Ranks agg will throw an exception due to missing a required param
                return ctor.apply(name, null, defaultConfig.get());
            }

            PercentilesConfig tDigestConfig = (PercentilesConfig) args[1];
            PercentilesConfig hdrConfig = (PercentilesConfig) args[2];

            @SuppressWarnings("unchecked")
            double[] values = args[0] != null ? ((List<Double>) args[0]).stream().mapToDouble(Double::doubleValue).toArray() : null;
            PercentilesConfig percentilesConfig;

            if (tDigestConfig != null && hdrConfig != null) {
                throw new IllegalArgumentException("Only one percentiles method should be declared.");
            } else if (tDigestConfig == null && hdrConfig == null) {
                percentilesConfig = defaultConfig.get();
            } else if (tDigestConfig != null) {
                percentilesConfig = tDigestConfig;
            } else {
                percentilesConfig = hdrConfig;
            }

            return ctor.apply(name, values, percentilesConfig);
        });

        ValuesSourceAggregationBuilder.declareFields(parser, true, true, false);
        parser.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), valuesField);
        parser.declareBoolean(T::keyed, KEYED_FIELD);
        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            PercentilesMethod.TDIGEST_PARSER,
            PercentilesMethod.TDIGEST.getParseField()
        );
        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            PercentilesMethod.HDR_PARSER,
            PercentilesMethod.HDR.getParseField()
        );

        return parser;
    }

    AbstractPercentilesAggregationBuilder(String name, double[] values, PercentilesConfig percentilesConfig, ParseField valuesField) {
        super(name);
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

    AbstractPercentilesAggregationBuilder(
        AbstractPercentilesAggregationBuilder<T> clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.percentilesConfig = clone.percentilesConfig;
        this.keyed = clone.keyed;
        this.values = clone.values;
        this.valuesField = clone.valuesField;
    }

    AbstractPercentilesAggregationBuilder(ParseField valuesField, StreamInput in) throws IOException {
        super(in);
        this.valuesField = valuesField;
        values = in.readDoubleArray();
        keyed = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            percentilesConfig = (PercentilesConfig) in.readOptionalWriteable((Reader<Writeable>) PercentilesConfig::fromStream);
        } else {
            int numberOfSignificantValueDigits = in.readVInt();
            double compression = in.readDouble();
            PercentilesMethod method = PercentilesMethod.readFromStream(in);
            percentilesConfig = PercentilesConfig.fromLegacy(method, compression, numberOfSignificantValueDigits);
        }
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(values);
        out.writeBoolean(keyed);
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeOptionalWriteable(percentilesConfig);
        } else {
            // Legacy method serialized both SigFigs and compression, even though we only need one. So we need
            // to serialize the default for the unused method
            int numberOfSignificantValueDigits = percentilesConfig.getMethod().equals(PercentilesMethod.HDR)
                ? ((PercentilesConfig.Hdr) percentilesConfig).getNumberOfSignificantValueDigits()
                : PercentilesConfig.Hdr.DEFAULT_NUMBER_SIG_FIGS;

            double compression = percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)
                ? ((PercentilesConfig.TDigest) percentilesConfig).getCompression()
                : PercentilesConfig.TDigest.DEFAULT_COMPRESSION;

            out.writeVInt(numberOfSignificantValueDigits);
            out.writeDouble(compression);
            percentilesConfig.getMethod().writeTo(out);
        }
    }

    /**
     * Set whether the XContent response should be keyed
     */
    @SuppressWarnings("unchecked")
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
     * Deprecated: set numberOfSignificantValueDigits by configuring a {@link PercentilesConfig.Hdr} instead
     * and set via {@link PercentilesAggregationBuilder#percentilesConfig(PercentilesConfig)}
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public T numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
        if (percentilesConfig == null || percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
            percentilesConfig = new PercentilesConfig.Hdr(numberOfSignificantValueDigits);
        } else {
            throw new IllegalArgumentException(
                "Cannot set [numberOfSignificantValueDigits] because the method " + "has already been configured for TDigest"
            );
        }

        return (T) this;
    }

    /**
     * Expert: get the number of significant digits in the values. Only relevant
     * when using {@link PercentilesMethod#HDR}.
     *
     * Deprecated: get numberOfSignificantValueDigits by inspecting the {@link PercentilesConfig} returned from
     * {@link PercentilesAggregationBuilder#percentilesConfig()} instead
     */
    @Deprecated
    public int numberOfSignificantValueDigits() {
        if (percentilesConfig != null && percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
            return ((PercentilesConfig.Hdr) percentilesConfig).getNumberOfSignificantValueDigits();
        }
        throw new IllegalStateException("Percentiles [method] has not been configured yet, or is a TDigest");
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     *
     * Deprecated: set compression by configuring a {@link PercentilesConfig.TDigest} instead
     * and set via {@link PercentilesAggregationBuilder#percentilesConfig(PercentilesConfig)}
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public T compression(double compression) {
        if (percentilesConfig == null || percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
            percentilesConfig = new PercentilesConfig.TDigest(compression);
        } else {
            throw new IllegalArgumentException("Cannot set [compression] because the method has already been configured for HDRHistogram");
        }
        return (T) this;
    }

    /**
     * Expert: get the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     *
     * Deprecated: get compression by inspecting the {@link PercentilesConfig} returned from
     * {@link PercentilesAggregationBuilder#percentilesConfig()} instead
     */
    @Deprecated
    public double compression() {
        if (percentilesConfig != null && percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
            return ((PercentilesConfig.TDigest) percentilesConfig).getCompression();
        }
        throw new IllegalStateException("Percentiles [method] has not been configured yet, or is a HdrHistogram");
    }

    /**
     * Deprecated: set method by configuring a {@link PercentilesConfig} instead
     * and set via {@link PercentilesAggregationBuilder#percentilesConfig(PercentilesConfig)}
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public T method(PercentilesMethod method) {
        if (method == null) {
            throw new IllegalArgumentException("[method] must not be null: [" + name + "]");
        }
        if (percentilesConfig == null) {
            if (method.equals(PercentilesMethod.TDIGEST)) {
                this.percentilesConfig = new PercentilesConfig.TDigest();
            } else {
                this.percentilesConfig = new PercentilesConfig.Hdr();
            }
        } else if (percentilesConfig.getMethod().equals(method) == false) {
            // we already have an algo configured, but it's different from the requested method
            // reset to default for the requested method
            if (method.equals(PercentilesMethod.TDIGEST)) {
                this.percentilesConfig = new PercentilesConfig.TDigest();
            } else {
                this.percentilesConfig = new PercentilesConfig.Hdr();
            }
        } // if method and config were same, this is a no-op so we don't overwrite settings

        return (T) this;
    }

    /**
     * Deprecated: get method by inspecting the {@link PercentilesConfig} returned from
     * {@link PercentilesAggregationBuilder#percentilesConfig()} instead
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
    public PercentilesConfig percentilesConfig() {
        return percentilesConfig;
    }

    /**
     * Sets how the percentiles algorithm should be configured
     */
    @SuppressWarnings("unchecked")
    public T percentilesConfig(PercentilesConfig percentilesConfig) {
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
    PercentilesConfig configOrDefault() {
        if (percentilesConfig == null) {
            return new PercentilesConfig.TDigest();
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

        AbstractPercentilesAggregationBuilder<?> other = (AbstractPercentilesAggregationBuilder<?>) obj;
        return Objects.deepEquals(values, other.values)
            && Objects.equals(keyed, other.keyed)
            && Objects.equals(configOrDefault(), other.configOrDefault());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(values), keyed, configOrDefault());
    }

    @Override
    public Set<String> metricNames() {
        return Arrays.stream(values).mapToObj(String::valueOf).collect(Collectors.toSet());
    }
}
