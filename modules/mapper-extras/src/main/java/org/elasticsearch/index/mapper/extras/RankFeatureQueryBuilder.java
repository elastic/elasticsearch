/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.extras.RankFeatureFieldMapper.RankFeatureFieldType;
import org.elasticsearch.index.mapper.extras.RankFeaturesFieldMapper.RankFeaturesFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Query to run on a [rank_feature] field.
 */
public final class RankFeatureQueryBuilder extends AbstractQueryBuilder<RankFeatureQueryBuilder> {
    private static final ScoreFunction DEFAULT_SCORE_FUNCTION = new ScoreFunction.Saturation();

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
                "log",
                a -> new Log((Float) a[0])
            );
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
            Query toQuery(String field, String feature, boolean positiveScoreImpact) {
                if (positiveScoreImpact == false) {
                    throw new IllegalArgumentException(
                        "Cannot use the [log] function with a field that has a negative score impact as "
                            + "it would trigger negative scores"
                    );
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
                "saturation",
                a -> new Saturation((Float) a[0])
            );
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
            Query toQuery(String field, String feature, boolean positiveScoreImpact) {
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
                "sigmoid",
                a -> new Sigmoid((Float) a[0], ((Float) a[1]).floatValue())
            );
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
                return pivot == that.pivot && exp == that.exp;
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
            Query toQuery(String field, String feature, boolean positiveScoreImpact) {
                return FeatureField.newSigmoidQuery(field, feature, DEFAULT_BOOST, pivot, exp);
            }
        }

        /**
         * A scoring function that scores documents as simply {@code S}
         * where S is the indexed value of the static feature.
         */
        public static class Linear extends ScoreFunction {

            private static final ObjectParser<Linear, Void> PARSER = new ObjectParser<>("linear", Linear::new);

            public Linear() {}

            private Linear(StreamInput in) {
                this();
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                return true;
            }

            @Override
            public int hashCode() {
                return getClass().hashCode();
            }

            @Override
            void writeTo(StreamOutput out) throws IOException {
                out.writeByte((byte) 3);
            }

            @Override
            void doXContent(XContentBuilder builder) throws IOException {
                builder.startObject("linear");
                builder.endObject();
            }

            @Override
            Query toQuery(String field, String feature, boolean positiveScoreImpact) {
                return FeatureField.newLinearQuery(field, feature, DEFAULT_BOOST);
            }
        }
    }

    private static ScoreFunction readScoreFunction(StreamInput in) throws IOException {
        byte b = in.readByte();
        return switch (b) {
            case 0 -> new ScoreFunction.Log(in);
            case 1 -> new ScoreFunction.Saturation(in);
            case 2 -> new ScoreFunction.Sigmoid(in);
            case 3 -> new ScoreFunction.Linear(in);
            default -> throw new IOException("Illegal score function id: " + b);
        };
    }

    public static final ConstructingObjectParser<RankFeatureQueryBuilder, Void> PARSER = new ConstructingObjectParser<>("feature", args -> {
        final String field = (String) args[0];
        final float boost = args[1] == null ? DEFAULT_BOOST : (Float) args[1];
        final String queryName = (String) args[2];
        long numNonNulls = Arrays.stream(args, 3, args.length).filter(Objects::nonNull).count();
        final RankFeatureQueryBuilder query;
        if (numNonNulls > 1) {
            throw new IllegalArgumentException("Can only specify one of [log], [saturation], [sigmoid] and [linear]");
        } else if (numNonNulls == 0) {
            query = new RankFeatureQueryBuilder(field, DEFAULT_SCORE_FUNCTION);
        } else {
            ScoreFunction scoreFunction = (ScoreFunction) Arrays.stream(args, 3, args.length).filter(Objects::nonNull).findAny().get();
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
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ScoreFunction.Log.PARSER, new ParseField("log"));
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            ScoreFunction.Saturation.PARSER,
            new ParseField("saturation")
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ScoreFunction.Sigmoid.PARSER, new ParseField("sigmoid"));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ScoreFunction.Linear.PARSER, new ParseField("linear"));
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
        if (false == scoreFunction.equals(DEFAULT_SCORE_FUNCTION)) {
            scoreFunction.doXContent(builder);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        final MappedFieldType ft = context.getFieldType(field);

        if (ft instanceof final RankFeatureFieldType fft) {
            return scoreFunction.toQuery(RankFeatureMetaFieldMapper.NAME, field, fft.positiveScoreImpact());
        } else if (ft == null) {
            final int lastDotIndex = field.lastIndexOf('.');
            if (lastDotIndex != -1) {
                final String parentField = field.substring(0, lastDotIndex);
                final MappedFieldType parentFt = context.getFieldType(parentField);
                if (parentFt instanceof RankFeaturesFieldType) {
                    return scoreFunction.toQuery(parentField, field.substring(lastDotIndex + 1), true);
                }
            }
            return new MatchNoDocsQuery(); // unmapped field
        } else {
            throw new IllegalArgumentException(
                "[rank_feature] query only works on [rank_feature] fields and "
                    + "features of [rank_features] fields, not ["
                    + ft.typeName()
                    + "]"
            );
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

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }
}
