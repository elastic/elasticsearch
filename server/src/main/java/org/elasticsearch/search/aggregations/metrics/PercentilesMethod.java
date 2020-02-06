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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An enum representing the methods for calculating percentiles
 */
public enum PercentilesMethod implements Writeable {
    /**
     * The TDigest method for calculating percentiles
     */
    TDIGEST("tdigest", "TDigest", "TDIGEST") {
        @Override
        Config configFromStream(StreamInput in) throws IOException {
            return new Config.TDigest(in);
        }
    },
    /**
     * The HDRHistogram method of calculating percentiles
     */
    HDR("hdr", "HDR") {
        @Override
        Config configFromStream(StreamInput in) throws IOException {
            return new Config.Hdr(in);
        }
    };

    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");
    public static final ParseField NUMBER_SIGNIFICANT_DIGITS_FIELD = new ParseField("number_of_significant_value_digits");

    public static final ObjectParser<Config.TDigest, String> TDIGEST_PARSER;
    static {
        TDIGEST_PARSER = new ObjectParser<>(PercentilesMethod.TDIGEST.getParseField().getPreferredName(), Config.TDigest::new);
        TDIGEST_PARSER.declareDouble(Config.TDigest::setCompression, COMPRESSION_FIELD);
    }

    public static final ObjectParser<Config.Hdr, String> HDR_PARSER;
    static {
        HDR_PARSER = new ObjectParser<>(PercentilesMethod.HDR.getParseField().getPreferredName(), Config.Hdr::new);
        HDR_PARSER.declareInt(Config.Hdr::setNumberOfSignificantValueDigits, NUMBER_SIGNIFICANT_DIGITS_FIELD);
    }

    private final ParseField parseField;

    PercentilesMethod(String name, String... deprecatedNames) {
        this.parseField = new ParseField(name, deprecatedNames);
    }

    abstract Config configFromStream(StreamInput in) throws IOException;

    /**
     * @return the name of the method
     */
    public ParseField getParseField() {
        return parseField;
    }

    public static PercentilesMethod readFromStream(StreamInput in) throws IOException {
        return in.readEnum(PercentilesMethod.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return parseField.getPreferredName();
    }

    /**
     * A small config object that carries algo-specific settings.  This allows the factory to have
     * a single unified constructor for both algos, but internally switch execution
     * depending on which algo is selected
     */
    public abstract static class Config implements ToXContent, Writeable {
        private final PercentilesMethod method;

        Config(PercentilesMethod method) {
            this.method = method;
        }

        public static Config fromStream(StreamInput in) throws IOException {
            PercentilesMethod method = PercentilesMethod.readFromStream(in);
            return method.configFromStream(in);
        }

        /**
         * Deprecated: construct a {@link PercentilesMethod.Config} directly instead
         */
        @Deprecated
        public static Config fromLegacy(PercentilesMethod method, double compression, int numberOfSignificantDigits) {
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

        abstract Aggregator createPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                        double[] values, boolean keyed, DocValueFormat formatter,
                                                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException;

        abstract Aggregator createPercentileRanksAggregator(String name, ValuesSource valuesSource, SearchContext context,
                                                            Aggregator parent, double[] values, boolean keyed,
                                                            DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                                                            Map<String, Object> metaData) throws IOException;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(method);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            Config other = (Config) obj;
            return method.equals(other.getMethod());
        }

        @Override
        public int hashCode() {
            return Objects.hash(method);
        }

        public static class TDigest extends Config {
            static final double DEFAULT_COMPRESSION = 100.0;
            private double compression;

            TDigest() {
                this(DEFAULT_COMPRESSION);
            }

            TDigest(double compression) {
                super(PercentilesMethod.TDIGEST);
                setCompression(compression);
            }

            private TDigest(StreamInput in) throws IOException {
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
            Aggregator createPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                   double[] values, boolean keyed, DocValueFormat formatter,
                                                   List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData) throws IOException {
                return new TDigestPercentilesAggregator(name, valuesSource, context, parent, values, compression, keyed, formatter,
                    pipelineAggregators, metaData);
            }

            @Override
            Aggregator createPercentileRanksAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                       double[] values, boolean keyed, DocValueFormat formatter,
                                                       List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData) throws IOException {
                return new TDigestPercentileRanksAggregator(name, valuesSource, context, parent, values, compression, keyed,
                    formatter, pipelineAggregators, metaData);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeDouble(compression);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject(getMethod().toString());
                builder.field(COMPRESSION_FIELD.getPreferredName(), compression);
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

        public static class Hdr extends Config {
            static final int DEFAULT_NUMBER_SIG_FIGS = 3;
            private int numberOfSignificantValueDigits;

            Hdr() {
                this(DEFAULT_NUMBER_SIG_FIGS);
            }

            Hdr(int numberOfSignificantValueDigits) {
                super(PercentilesMethod.HDR);
                setNumberOfSignificantValueDigits(numberOfSignificantValueDigits);
            }

            private Hdr(StreamInput in) throws IOException {
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
            Aggregator createPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                   double[] values, boolean keyed, DocValueFormat formatter,
                                                   List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData) throws IOException {
                return new HDRPercentilesAggregator(name, valuesSource, context, parent, values, numberOfSignificantValueDigits, keyed,
                    formatter, pipelineAggregators, metaData);
            }

            @Override
            Aggregator createPercentileRanksAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                                       double[] values, boolean keyed, DocValueFormat formatter,
                                                       List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData) throws IOException {
                return new HDRPercentileRanksAggregator(name, valuesSource, context, parent, values, numberOfSignificantValueDigits, keyed,
                    formatter, pipelineAggregators, metaData);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeVInt(numberOfSignificantValueDigits);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject(getMethod().toString());
                builder.field(NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
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
}
