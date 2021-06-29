/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ConfusionMatrixMetric extends AbstractConfusionMatrixMetric {

    public static final String NAME = "confusion_matrix";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ConfusionMatrixMetric, Void> PARSER =
        new ConstructingObjectParser<>(NAME, args -> new ConfusionMatrixMetric((List<Double>) args[0]));

    static {
        PARSER.declareDoubleArray(constructorArg(), AT);
    }

    public static ConfusionMatrixMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static ConfusionMatrixMetric at(Double... at) {
        return new ConfusionMatrixMetric(Arrays.asList(at));
    }

    public ConfusionMatrixMetric(List<Double> at) {
        super(at);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfusionMatrixMetric that = (ConfusionMatrixMetric) o;
        return Arrays.equals(thresholds, that.thresholds);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(thresholds);
    }

    public static class Result implements EvaluationMetric.Result {

        public static Result fromXContent(XContentParser parser) throws IOException {
            return new Result(parser.map(LinkedHashMap::new, ConfusionMatrix::fromXContent));
        }

        private final Map<String, ConfusionMatrix> results;

        public Result(Map<String, ConfusionMatrix> results) {
            this.results = Objects.requireNonNull(results);
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public ConfusionMatrix getScoreByThreshold(String threshold) {
            return results.get(threshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return builder.map(results);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(results, that.results);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final class ConfusionMatrix implements ToXContentObject {

        public static ConfusionMatrix fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private static final ParseField TP = new ParseField("tp");
        private static final ParseField FP = new ParseField("fp");
        private static final ParseField TN = new ParseField("tn");
        private static final ParseField FN = new ParseField("fn");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ConfusionMatrix, Void> PARSER =
            new ConstructingObjectParser<>(
                "confusion_matrix", true, args -> new ConfusionMatrix((long) args[0], (long) args[1], (long) args[2], (long) args[3]));

        static {
            PARSER.declareLong(constructorArg(), TP);
            PARSER.declareLong(constructorArg(), FP);
            PARSER.declareLong(constructorArg(), TN);
            PARSER.declareLong(constructorArg(), FN);
        }

        private final long tp;
        private final long fp;
        private final long tn;
        private final long fn;

        public ConfusionMatrix(long tp, long fp, long tn, long fn) {
            this.tp = tp;
            this.fp = fp;
            this.tn = tn;
            this.fn = fn;
        }

        public long getTruePositives() {
            return tp;
        }

        public long getFalsePositives() {
            return fp;
        }

        public long getTrueNegatives() {
            return tn;
        }

        public long getFalseNegatives() {
            return fn;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder
                .startObject()
                .field(TP.getPreferredName(), tp)
                .field(FP.getPreferredName(), fp)
                .field(TN.getPreferredName(), tn)
                .field(FN.getPreferredName(), fn)
                .endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConfusionMatrix that = (ConfusionMatrix) o;
            return tp == that.tp && fp == that.fp && tn == that.tn && fn == that.fn;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tp, fp, tn, fn);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
