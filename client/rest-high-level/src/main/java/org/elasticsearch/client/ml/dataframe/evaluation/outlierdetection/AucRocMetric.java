/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Area under the curve (AUC) of the receiver operating characteristic (ROC).
 * The ROC curve is a plot of the TPR (true positive rate) against
 * the FPR (false positive rate) over a varying threshold.
 */
public class AucRocMetric implements EvaluationMetric {

    public static final String NAME = "auc_roc";

    public static final ParseField INCLUDE_CURVE = new ParseField("include_curve");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<AucRocMetric, Void> PARSER =
        new ConstructingObjectParser<>(NAME, args -> new AucRocMetric((Boolean) args[0]));

    static {
        PARSER.declareBoolean(optionalConstructorArg(), INCLUDE_CURVE);
    }

    public static AucRocMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static AucRocMetric withCurve() {
        return new AucRocMetric(true);
    }

    private final boolean includeCurve;

    public AucRocMetric(Boolean includeCurve) {
        this.includeCurve = includeCurve == null ? false : includeCurve;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder
            .startObject()
            .field(INCLUDE_CURVE.getPreferredName(), includeCurve)
            .endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AucRocMetric that = (AucRocMetric) o;
        return Objects.equals(includeCurve, that.includeCurve);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeCurve);
    }

    public static class Result implements EvaluationMetric.Result {

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private static final ParseField SCORE = new ParseField("score");
        private static final ParseField CURVE = new ParseField("curve");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>("auc_roc_result", true, args -> new Result((double) args[0], (List<AucRocPoint>) args[1]));

        static {
            PARSER.declareDouble(constructorArg(), SCORE);
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> AucRocPoint.fromXContent(p), CURVE);
        }

        private final double score;
        private final List<AucRocPoint> curve;

        public Result(double score, @Nullable List<AucRocPoint> curve) {
            this.score = score;
            this.curve = curve;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public double getScore() {
            return score;
        }

        public List<AucRocPoint> getCurve() {
            return curve == null ? null : Collections.unmodifiableList(curve);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(SCORE.getPreferredName(), score);
            if (curve != null && curve.isEmpty() == false) {
                builder.field(CURVE.getPreferredName(), curve);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(score, that.score)
                && Objects.equals(curve, that.curve);
        }

        @Override
        public int hashCode() {
            return Objects.hash(score, curve);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final class AucRocPoint implements ToXContentObject {

        public static AucRocPoint fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private static final ParseField TPR = new ParseField("tpr");
        private static final ParseField FPR = new ParseField("fpr");
        private static final ParseField THRESHOLD = new ParseField("threshold");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AucRocPoint, Void> PARSER =
            new ConstructingObjectParser<>(
                "auc_roc_point",
                true,
                args -> new AucRocPoint((double) args[0], (double) args[1], (double) args[2]));

        static {
            PARSER.declareDouble(constructorArg(), TPR);
            PARSER.declareDouble(constructorArg(), FPR);
            PARSER.declareDouble(constructorArg(), THRESHOLD);
        }

        private final double tpr;
        private final double fpr;
        private final double threshold;

        public AucRocPoint(double tpr, double fpr, double threshold) {
            this.tpr = tpr;
            this.fpr = fpr;
            this.threshold = threshold;
        }

        public double getTruePositiveRate() {
            return tpr;
        }

        public double getFalsePositiveRate() {
            return fpr;
        }

        public double getThreshold() {
            return threshold;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder
                .startObject()
                .field(TPR.getPreferredName(), tpr)
                .field(FPR.getPreferredName(), fpr)
                .field(THRESHOLD.getPreferredName(), threshold)
                .endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AucRocPoint that = (AucRocPoint) o;
            return tpr == that.tpr && fpr == that.fpr && threshold == that.threshold;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tpr, fpr, threshold);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
