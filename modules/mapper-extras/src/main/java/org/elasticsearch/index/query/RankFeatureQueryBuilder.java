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

package org.elasticsearch.index.query;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.RankFeatureFieldMapper.RankFeatureFieldType;
import org.elasticsearch.index.mapper.RankFeatureMetaFieldMapper;
import org.elasticsearch.index.mapper.RankFeaturesFieldMapper.RankFeaturesFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Query to run on a [rank_feature] field.
 */
public final class RankFeatureQueryBuilder extends AbstractQueryBuilder<RankFeatureQueryBuilder> {

    /**
     * Scoring function for a [rank_feature] field.
     */
    public abstract static class ScoreFunction {

        private ScoreFunction() {} // prevent extensions by users

        abstract void writeTo(StreamOutput out) throws IOException;

        abstract Query toQuery(String field, String feature, boolean positiveScoreImpact) throws IOException;

        abstract void doXContent(XContentBuilder builder) throws IOException;

        /**
         * A scoring function that scores documents as {@code Math.log(scalingFactor + S)}
         * where S is the value of the static feature.
         */
        public static class Log extends ScoreFunction {

            private static final ConstructingObjectParser<Log, Void> PARSER = new ConstructingObjectParser<>(
                    "log", a -> new Log((Float) a[0]));
            static {
                PARSER.declareFloat(ConstructingObjectParser.constructorArg(), new ParseField("scaling_factor"));
            }

            private final float scalingFactor;

            public Log(float scalingFactor) {
                this.scalingFactor = scalingFactor;
            }

            private Log(StreamInput in) throws IOException {
                this(in.readFloat());
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != getClass()) {
                    return false;
                }
                Log that = (Log) obj;
                return scalingFactor == that.scalingFactor;
            }

            @Override
            public int hashCode() {
                return Float.hashCode(scalingFactor);
            }

            @Override
            void writeTo(StreamOutput out) throws IOException {
                out.writeByte((byte) 0);
                out.writeFloat(scalingFactor);
            }

            @Override
            void doXContent(XContentBuilder builder) throws IOException {
                builder.startObject("log");
                builder.field("scaling_factor", scalingFactor);
                builder.endObject();
            }

            @Override
            Query toQuery(String field, String feature, boolean positiveScoreImpact) throws IOException {
                if (positiveScoreImpact == false) {
                    throw new IllegalArgumentException("Cannot use the [log] function with a field that has a negative score impact as " +
                            "it would trigger negative scores");
                }
                return FeatureField.newLogQuery(field, feature, DEFAULT_BOOST, scalingFactor);
            }
        }

        /**
         * A scoring function that scores documents as {@code S / (S + pivot)} where S is
         * the value of the static feature.
         */
        public static class Saturation extends ScoreFunction {

            private static final ConstructingObjectParser<Saturation, Void> PARSER = new ConstructingObjectParser<>(
                    "saturation", a -> new Saturation((Float) a[0]));
            static {
                PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), new ParseField("pivot"));
            }

            private final Float pivot;

            /** Constructor with a default pivot, computed as the geometric average of
             *  all feature values in the index. */
            public Saturation() {
                this((Float) null);
            }

            public Saturation(float pivot) {
                this(Float.valueOf(pivot));
            }

            private Saturation(Float pivot) {
                this.pivot = pivot;
            }

            private Saturation(StreamInput in) throws IOException {
                this(in.readOptionalFloat());
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != getClass()) {
                    return false;
                }
                Saturation that = (Saturation) obj;
                return Objects.equals(pivot, that.pivot);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(pivot);
            }

            @Override
            void writeTo(StreamOutput out) throws IOException {
                out.writeByte((byte) 1);
                out.writeOptionalFloat(pivot);
            }

            @Override
            void doXContent(XContentBuilder builder) throws IOException {
                builder.startObject("saturation");
                if (pivot != null) {
                    builder.field("pivot", pivot);
                }
                builder.endObject();
            }

            @Override
            Query toQuery(String field, String feature, boolean positiveScoreImpact) throws IOException {
                if (pivot == null) {
                    return FeatureField.newSaturationQuery(field, feature);
                } else {
                    return FeatureField.newSaturationQuery(field, feature, DEFAULT_BOOST, pivot);
                }
            }
        }

        /**
         * A scoring function that scores documents as {@code S^exp / (S^exp + pivot^exp)}
         * where S is the value of the static feature.
         */
        public static class Sigmoid extends ScoreFunction {

            private static final ConstructingObjectParser<Sigmoid, Void> PARSER = new ConstructingObjectParser<>(
                    "sigmoid", a -> new Sigmoid((Float) a[0], ((Float) a[1]).floatValue()));
            static {
                PARSER.declareFloat(ConstructingObjectParser.constructorArg(), new ParseField("pivot"));
                PARSER.declareFloat(ConstructingObjectParser.constructorArg(), new ParseField("exponent"));
            }

            private final float pivot;
            private final float exp;

            public Sigmoid(float pivot, float exp) {
                this.pivot = pivot;
                this.exp = exp;
            }

            private Sigmoid(StreamInput in) throws IOException {
                this(in.readFloat(), in.readFloat());
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != getClass()) {
                    return false;
                }
                Sigmoid that = (Sigmoid) obj;
                return pivot == that.pivot
                        && exp == that.exp;
            }

            @Override
            public int hashCode() {
                return Objects.hash(pivot, exp);
            }

            @Override
            void writeTo(StreamOutput out) throws IOException {
                out.writeByte((byte) 2);
                out.writeFloat(pivot);
                out.writeFloat(exp);
            }

            @Override
            void doXContent(XContentBuilder builder) throws IOException {
                builder.startObject("sigmoid");
                builder.field("pivot", pivot);
                builder.field("exponent", exp);
                builder.endObject();
            }

            @Override
            Query toQuery(String field, String feature, boolean positiveScoreImpact) throws IOException {
                return FeatureField.newSigmoidQuery(field, feature, DEFAULT_BOOST, pivot, exp);
            }
        }
    }

    private static ScoreFunction readScoreFunction(StreamInput in) throws IOException {
        byte b = in.readByte();
        switch (b) {
        case 0:
            return new ScoreFunction.Log(in);
        case 1:
            return new ScoreFunction.Saturation(in);
        case 2:
            return new ScoreFunction.Sigmoid(in);
        default:
            throw new IOException("Illegal score function id: " + b);
        }
    }

    public static final ConstructingObjectParser<RankFeatureQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
            "feature", args -> {
                final String field = (String) args[0];
                final float boost = args[1] == null ? DEFAULT_BOOST : (Float) args[1];
                final String queryName = (String) args[2];
                long numNonNulls = Arrays.stream(args, 3, args.length).filter(Objects::nonNull).count();
                final RankFeatureQueryBuilder query;
                if (numNonNulls > 1) {
                    throw new IllegalArgumentException("Can only specify one of [log], [saturation] and [sigmoid]");
                } else if (numNonNulls == 0) {
                    query = new RankFeatureQueryBuilder(field, new ScoreFunction.Saturation());
                } else {
                    ScoreFunction scoreFunction = (ScoreFunction) Arrays.stream(args, 3, args.length)
                            .filter(Objects::nonNull)
                            .findAny()
                            .get();
                    query = new RankFeatureQueryBuilder(field, scoreFunction);
                }
                query.boost(boost);
                query.queryName(queryName);
                return query;
            });
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("field"));
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), BOOST_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                ScoreFunction.Log.PARSER, new ParseField("log"));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                ScoreFunction.Saturation.PARSER, new ParseField("saturation"));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                ScoreFunction.Sigmoid.PARSER, new ParseField("sigmoid"));
    }

    public static final String NAME = "rank_feature";

    private final String field;
    private final ScoreFunction scoreFunction;

    /**
     *
     * @param field  The field name.
     * @param scoreFunction Scoring function for the rank_feature field.
     */
    public RankFeatureQueryBuilder(String field, ScoreFunction scoreFunction) {
        this.field = Objects.requireNonNull(field);
        this.scoreFunction = Objects.requireNonNull(scoreFunction);
    }

    public RankFeatureQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.scoreFunction = readScoreFunction(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        scoreFunction.writeTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("field", field);
        scoreFunction.doXContent(builder);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(field);

        if (ft instanceof RankFeatureFieldType) {
            final RankFeatureFieldType fft = (RankFeatureFieldType) ft;
            return scoreFunction.toQuery(RankFeatureMetaFieldMapper.NAME, field, fft.positiveScoreImpact());
        } else if (ft == null) {
            final int lastDotIndex = field.lastIndexOf('.');
            if (lastDotIndex != -1) {
                final String parentField = field.substring(0, lastDotIndex);
                final MappedFieldType parentFt = context.fieldMapper(parentField);
                if (parentFt instanceof RankFeaturesFieldType) {
                    return scoreFunction.toQuery(parentField, field.substring(lastDotIndex + 1), true);
                }
            }
            return new MatchNoDocsQuery(); // unmapped field
        } else {
            throw new IllegalArgumentException("[rank_feature] query only works on [rank_feature] fields and " +
                "features of [rank_features] fields, not [" + ft.typeName() + "]");
        }
    }

    @Override
    protected boolean doEquals(RankFeatureQueryBuilder other) {
        return Objects.equals(field, other.field) && Objects.equals(scoreFunction, other.scoreFunction);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, scoreFunction);
    }

}
