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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * An enum representing the methods for calculating percentiles
 */
public enum PercentilesMethod implements Writeable {
    /**
     * The TDigest method for calculating percentiles
     */
    TDIGEST("tdigest", "TDigest", "TDIGEST"),
    /**
     * The HDRHistogram method of calculating percentiles
     */
    HDR("hdr", "HDR");

    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");
    public static final ParseField NUMBER_SIGNIFICANT_DIGITS_FIELD = new ParseField("number_of_significant_value_digits");

    public static final ConstructingObjectParser<Config.TDigest, String> TDIGEST_PARSER;
    static {
        TDIGEST_PARSER = new ConstructingObjectParser<>(PercentilesMethod.TDIGEST.getParseField().getPreferredName(), false,
            objects -> new Config.TDigest((double) objects[0]));
        TDIGEST_PARSER.declareDouble(ConstructingObjectParser.constructorArg(), COMPRESSION_FIELD);
    }

    public static final ConstructingObjectParser<Config.Hdr, String> HDR_PARSER;
    static {
        HDR_PARSER = new ConstructingObjectParser<>(PercentilesMethod.HDR.getParseField().getPreferredName(), false,
            objects -> new Config.Hdr((int) objects[0]));
        HDR_PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_SIGNIFICANT_DIGITS_FIELD);
    }

    private final ParseField parseField;

    PercentilesMethod(String name, String... deprecatedNames) {
        this.parseField = new ParseField(name, deprecatedNames);
    }

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
            if (method.equals(PercentilesMethod.TDIGEST)) {
                return new TDigest(in);
            } else if (method.equals(PercentilesMethod.HDR)) {
                return new Hdr(in);
            }
            throw new IllegalArgumentException("Unsupported percentiles algorithm [" + method + "]");
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
            private final double compression;

            TDigest() {
                this(DEFAULT_COMPRESSION);
            }

            TDigest(double compression) {
                super(PercentilesMethod.TDIGEST);
                this.compression = compression;
            }

            private TDigest(StreamInput in) throws IOException {
                this(in.readDouble());
            }

            public double getCompression() {
                return compression;
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
            private final int numberOfSignificantValueDigits;

            Hdr() {
                this(DEFAULT_NUMBER_SIG_FIGS);
            }

            Hdr(int numberOfSignificantValueDigits) {
                super(PercentilesMethod.HDR);
                this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
            }

            private Hdr(StreamInput in) throws IOException {
                this(in.readVInt());
            }

            public int getNumberOfSignificantValueDigits() {
                return numberOfSignificantValueDigits;
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
