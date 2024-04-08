/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A small config object that carries algo-specific settings.  This allows the factory to have
 * a single unified constructor for both algos, but internally switch execution
 * depending on which algo is selected
 */
public abstract class PercentilesConfig implements ToXContent, Writeable {

    public static int indexOfKey(double[] keys, double key) {
        return ArrayUtils.binarySearch(keys, key, 0.001);
    }

    private final PercentilesMethod method;

    PercentilesConfig(PercentilesMethod method) {
        this.method = method;
    }

    public static PercentilesConfig fromStream(StreamInput in) throws IOException {
        PercentilesMethod method = PercentilesMethod.readFromStream(in);
        return method.configFromStream(in);
    }

    public PercentilesMethod getMethod() {
        return method;
    }

    public abstract Aggregator createPercentilesAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] values,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException;

    public abstract InternalNumericMetricsAggregation.MultiValue createEmptyPercentilesAggregator(
        String name,
        double[] values,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    );

    abstract Aggregator createPercentileRanksAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] values,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException;

    public abstract InternalNumericMetricsAggregation.MultiValue createEmptyPercentileRanksAggregator(
        String name,
        double[] values,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    );

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(method);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        PercentilesConfig other = (PercentilesConfig) obj;
        return method.equals(other.getMethod());
    }

    @Override
    public int hashCode() {
        return Objects.hash(method);
    }

    public static final class TDigest extends PercentilesConfig {
        static final double DEFAULT_COMPRESSION = 100.0;
        private double compression;

        private TDigestExecutionHint executionHint;

        public TDigest() {
            this(DEFAULT_COMPRESSION);
        }

        public TDigest(double compression) {
            this(compression, null);
        }

        public TDigest(double compression, TDigestExecutionHint executionHint) {
            super(PercentilesMethod.TDIGEST);
            this.executionHint = executionHint;
            setCompression(compression);
        }

        TDigest(StreamInput in) throws IOException {
            this(
                in.readDouble(),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)
                    ? in.readOptionalWriteable(TDigestExecutionHint::readFrom)
                    : TDigestExecutionHint.HIGH_ACCURACY
            );
        }

        public void setCompression(double compression) {
            if (compression < 0.0) {
                throw new IllegalArgumentException("[compression] must be greater than or equal to 0. Found [" + compression + "]");
            }
            this.compression = compression;
        }

        public double getCompression() {
            return compression;
        }

        public void parseExecutionHint(String executionHint) {
            this.executionHint = TDigestExecutionHint.parse(executionHint);
        }

        public TDigestExecutionHint getExecutionHint(AggregationContext context) {
            if (executionHint == null) {
                executionHint = TDigestExecutionHint.parse(context.getClusterSettings().get(TDigestExecutionHint.SETTING));
            }
            return executionHint;
        }

        @Override
        public Aggregator createPercentilesAggregator(
            String name,
            ValuesSourceConfig config,
            AggregationContext context,
            Aggregator parent,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) throws IOException {
            return new TDigestPercentilesAggregator(
                name,
                config,
                context,
                parent,
                values,
                compression,
                getExecutionHint(context),
                keyed,
                formatter,
                metadata
            );
        }

        @Override
        public InternalNumericMetricsAggregation.MultiValue createEmptyPercentilesAggregator(
            String name,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) {
            return InternalTDigestPercentiles.empty(name, values, keyed, formatter, metadata);
        }

        @Override
        Aggregator createPercentileRanksAggregator(
            String name,
            ValuesSourceConfig config,
            AggregationContext context,
            Aggregator parent,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) throws IOException {
            return new TDigestPercentileRanksAggregator(
                name,
                config,
                context,
                parent,
                values,
                compression,
                getExecutionHint(context),
                keyed,
                formatter,
                metadata
            );
        }

        @Override
        public InternalNumericMetricsAggregation.MultiValue createEmptyPercentileRanksAggregator(
            String name,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) {
            TDigestExecutionHint hint = executionHint == null ? TDigestExecutionHint.DEFAULT : executionHint;
            return InternalTDigestPercentileRanks.empty(name, values, compression, hint, keyed, formatter, metadata);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeDouble(compression);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeOptionalWriteable(executionHint);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getMethod().toString());
            builder.field(PercentilesMethod.COMPRESSION_FIELD.getPreferredName(), compression);
            if (executionHint != null) {
                builder.field(PercentilesMethod.EXECUTION_HINT_FIELD.getPreferredName(), executionHint);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            if (super.equals(obj) == false) return false;

            TDigest other = (TDigest) obj;
            return compression == other.getCompression() && Objects.equals(executionHint, other.executionHint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), compression, executionHint);
        }
    }

    public static final class Hdr extends PercentilesConfig {
        static final int DEFAULT_NUMBER_SIG_FIGS = 3;
        private int numberOfSignificantValueDigits;

        public Hdr() {
            this(DEFAULT_NUMBER_SIG_FIGS);
        }

        public Hdr(int numberOfSignificantValueDigits) {
            super(PercentilesMethod.HDR);
            setNumberOfSignificantValueDigits(numberOfSignificantValueDigits);
        }

        Hdr(StreamInput in) throws IOException {
            this(in.readVInt());
        }

        public void setNumberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
            if (numberOfSignificantValueDigits < 0 || numberOfSignificantValueDigits > 5) {
                throw new IllegalArgumentException("[numberOfSignificantValueDigits] must be between 0 and 5");
            }
            this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        }

        public int getNumberOfSignificantValueDigits() {
            return numberOfSignificantValueDigits;
        }

        @Override
        public Aggregator createPercentilesAggregator(
            String name,
            ValuesSourceConfig config,
            AggregationContext context,
            Aggregator parent,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) throws IOException {
            return new HDRPercentilesAggregator(
                name,
                config,
                context,
                parent,
                values,
                numberOfSignificantValueDigits,
                keyed,
                formatter,
                metadata
            );
        }

        @Override
        public InternalNumericMetricsAggregation.MultiValue createEmptyPercentilesAggregator(
            String name,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) {
            return InternalHDRPercentiles.empty(name, values, keyed, formatter, metadata);
        }

        @Override
        Aggregator createPercentileRanksAggregator(
            String name,
            ValuesSourceConfig config,
            AggregationContext context,
            Aggregator parent,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) throws IOException {
            return new HDRPercentileRanksAggregator(
                name,
                config,
                context,
                parent,
                values,
                numberOfSignificantValueDigits,
                keyed,
                formatter,
                metadata
            );
        }

        @Override
        public InternalNumericMetricsAggregation.MultiValue createEmptyPercentileRanksAggregator(
            String name,
            double[] values,
            boolean keyed,
            DocValueFormat formatter,
            Map<String, Object> metadata
        ) {
            return InternalHDRPercentileRanks.empty(name, values, keyed, formatter, metadata);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(numberOfSignificantValueDigits);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getMethod().toString());
            builder.field(PercentilesMethod.NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            if (super.equals(obj) == false) return false;

            Hdr other = (Hdr) obj;
            return numberOfSignificantValueDigits == other.getNumberOfSignificantValueDigits();
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), numberOfSignificantValueDigits);
        }
    }
}
