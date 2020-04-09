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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A small config object that carries algo-specific settings.  This allows the factory to have
 * a single unified constructor for both algos, but internally switch execution
 * depending on which algo is selected
 */
public abstract class PercentilesConfig implements ToXContent, Writeable {
    private final PercentilesMethod method;

    PercentilesConfig(PercentilesMethod method) {
        this.method = method;
    }

    public static PercentilesConfig fromStream(StreamInput in) throws IOException {
        PercentilesMethod method = PercentilesMethod.readFromStream(in);
        return method.configFromStream(in);
    }

    /**
     * Deprecated: construct a {@link PercentilesConfig} directly instead
     */
    @Deprecated
    public static PercentilesConfig fromLegacy(PercentilesMethod method, double compression, int numberOfSignificantDigits) {
        if (method.equals(PercentilesMethod.TDIGEST)) {
            return new TDigest(compression);
        } else if (method.equals(PercentilesMethod.HDR)) {
            return new Hdr(numberOfSignificantDigits);
        }
        throw new IllegalArgumentException("Unsupported percentiles algorithm [" + method + "]");
    }

    public PercentilesMethod getMethod() {
        return method;
    }

    public abstract Aggregator createPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                           double[] values, boolean keyed, DocValueFormat formatter,
                                                           Map<String, Object> metadata) throws IOException;

    abstract Aggregator createPercentileRanksAggregator(String name, ValuesSource valuesSource, SearchContext context,
                                                        Aggregator parent, double[] values, boolean keyed,
                                                        DocValueFormat formatter, Map<String, Object> metadata) throws IOException;

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

    public static class TDigest extends PercentilesConfig {
        static final double DEFAULT_COMPRESSION = 100.0;
        private double compression;

        public TDigest() {
            this(DEFAULT_COMPRESSION);
        }

        public TDigest(double compression) {
            super(PercentilesMethod.TDIGEST);
            setCompression(compression);
        }

        TDigest(StreamInput in) throws IOException {
            this(in.readDouble());
        }

        public void setCompression(double compression) {
            if (compression < 0.0) {
                throw new IllegalArgumentException(
                    "[compression] must be greater than or equal to 0. Found [" + compression + "]");
            }
            this.compression = compression;
        }

        public double getCompression() {
            return compression;
        }

        @Override
        public Aggregator createPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                      double[] values, boolean keyed, DocValueFormat formatter,
                                                      Map<String, Object> metadata) throws IOException {
            return new TDigestPercentilesAggregator(name, valuesSource, context, parent, values, compression, keyed, formatter, metadata);
        }

        @Override
        Aggregator createPercentileRanksAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                   double[] values, boolean keyed, DocValueFormat formatter,
                                                   Map<String, Object> metadata) throws IOException {
            return new TDigestPercentileRanksAggregator(name, valuesSource, context, parent, values, compression, keyed,
                formatter, metadata);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeDouble(compression);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getMethod().toString());
            builder.field(PercentilesMethod.COMPRESSION_FIELD.getPreferredName(), compression);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            if (super.equals(obj) == false) return false;

            TDigest other = (TDigest) obj;
            return compression == other.getCompression();
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), compression);
        }
    }

    public static class Hdr extends PercentilesConfig {
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
        public Aggregator createPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                      double[] values, boolean keyed, DocValueFormat formatter,
                                                      Map<String, Object> metadata) throws IOException {
            return new HDRPercentilesAggregator(name, valuesSource, context, parent, values, numberOfSignificantValueDigits, keyed,
                formatter, metadata);
        }

        @Override
        Aggregator createPercentileRanksAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                   double[] values, boolean keyed, DocValueFormat formatter,
                                                   Map<String, Object> metadata) throws IOException {
            return new HDRPercentileRanksAggregator(name, valuesSource, context, parent, values, numberOfSignificantValueDigits, keyed,
                formatter, metadata);
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
