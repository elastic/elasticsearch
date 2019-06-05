/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Area under the curve (AUC) of the receiver operating characteristic (ROC).
 * The ROC curve is a plot of the TPR (true positive rate) against
 * the FPR (false positive rate) over a varying threshold.
 *
 * This particular implementation is making use of ES aggregations
 * to calculate the curve. It then uses the trapezoidal rule to calculate
 * the AUC.
 *
 * In particular, in order to calculate the ROC, we get percentiles of TP
 * and FP against the predicted probability. We call those Rate-Threshold
 * curves. We then scan ROC points from each Rate-Threshold curve against the
 * other using interpolation. This gives us an approximation of the ROC curve
 * that has the advantage of being efficient and resilient to some edge cases.
 *
 * When this is used for multi-class classification, it will calculate the ROC
 * curve of each class versus the rest.
 */
public class AucRoc implements SoftClassificationMetric {

    public static final ParseField NAME = new ParseField("auc_roc");

    public static final ParseField INCLUDE_CURVE = new ParseField("include_curve");

    public static final ConstructingObjectParser<AucRoc, Void> PARSER = new ConstructingObjectParser<>(NAME.getPreferredName(),
        a -> new AucRoc((Boolean) a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), INCLUDE_CURVE);
    }

    private static final String PERCENTILES = "percentiles";

    public static AucRoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final boolean includeCurve;

    public AucRoc(Boolean includeCurve) {
        this.includeCurve = includeCurve == null ? false : includeCurve;
    }

    public AucRoc(StreamInput in) throws IOException {
        this.includeCurve = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(includeCurve);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INCLUDE_CURVE.getPreferredName(), includeCurve);
        builder.endObject();
        return builder;
    }

    @Override
    public String getMetricName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AucRoc that = (AucRoc) o;
        return Objects.equals(includeCurve, that.includeCurve);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeCurve);
    }

    @Override
    public List<AggregationBuilder> aggs(String actualField, List<ClassInfo> classInfos) {
        double[] percentiles = IntStream.range(1, 100).mapToDouble(v -> (double) v).toArray();
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (ClassInfo classInfo : classInfos) {
            AggregationBuilder percentilesForClassValueAgg = AggregationBuilders
                .filter(evaluatedLabelAggName(classInfo), classInfo.matchingQuery())
                .subAggregation(
                    AggregationBuilders.percentiles(PERCENTILES).field(classInfo.getProbabilityField()).percentiles(percentiles));
            AggregationBuilder percentilesForRestAgg = AggregationBuilders
                .filter(restLabelsAggName(classInfo), QueryBuilders.boolQuery().mustNot(classInfo.matchingQuery()))
                .subAggregation(
                    AggregationBuilders.percentiles(PERCENTILES).field(classInfo.getProbabilityField()).percentiles(percentiles));
            aggs.add(percentilesForClassValueAgg);
            aggs.add(percentilesForRestAgg);
        }
        return aggs;
    }

    private String evaluatedLabelAggName(ClassInfo classInfo) {
        return getMetricName() + "_" + classInfo.getName();
    }

    private String restLabelsAggName(ClassInfo classInfo) {
        return getMetricName() + "_non_" + classInfo.getName();
    }

    @Override
    public EvaluationMetricResult evaluate(ClassInfo classInfo, Aggregations aggs) {
        Filter classAgg = aggs.get(evaluatedLabelAggName(classInfo));
        Filter restAgg = aggs.get(restLabelsAggName(classInfo));
        double[] tpPercentiles = percentilesArray(classAgg.getAggregations().get(PERCENTILES),
            "[" + getMetricName() + "] requires at least one actual_field to have the value [" + classInfo.getName() + "]");
        double[] fpPercentiles = percentilesArray(restAgg.getAggregations().get(PERCENTILES),
            "[" + getMetricName() + "] requires at least one actual_field to have a different value than [" + classInfo.getName() + "]");
        List<AucRocPoint> aucRocCurve = buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = calculateAucScore(aucRocCurve);
        return new Result(aucRocScore, includeCurve ? aucRocCurve : Collections.emptyList());
    }

    private static double[] percentilesArray(Percentiles percentiles, String errorIfUndefined) {
        double[] result = new double[99];
        percentiles.forEach(percentile -> {
            if (Double.isNaN(percentile.getValue())) {
                throw ExceptionsHelper.badRequestException(errorIfUndefined);
            }
            result[((int) percentile.getPercent()) - 1] = percentile.getValue();
        });
        return result;
    }

    /**
     * Visible for testing
     */
    static List<AucRocPoint> buildAucRocCurve(double[] tpPercentiles, double[] fpPercentiles) {
        assert tpPercentiles.length == fpPercentiles.length;
        assert tpPercentiles.length == 99;

        List<AucRocPoint> aucRocCurve = new ArrayList<>();
        aucRocCurve.add(new AucRocPoint(0.0, 0.0, 1.0));
        aucRocCurve.add(new AucRocPoint(1.0, 1.0, 0.0));
        RateThresholdCurve tpCurve = new RateThresholdCurve(tpPercentiles, true);
        RateThresholdCurve fpCurve = new RateThresholdCurve(fpPercentiles, false);
        aucRocCurve.addAll(tpCurve.scanPoints(fpCurve));
        aucRocCurve.addAll(fpCurve.scanPoints(tpCurve));
        Collections.sort(aucRocCurve);
        return aucRocCurve;
    }

    /**
     * Visible for testing
     */
    static double calculateAucScore(List<AucRocPoint> rocCurve) {
        // Calculates AUC based on the trapezoid rule
        double aucRoc = 0.0;
        for (int i = 1; i < rocCurve.size(); i++) {
            AucRocPoint left = rocCurve.get(i - 1);
            AucRocPoint right = rocCurve.get(i);
            aucRoc += (right.fpr - left.fpr) * (right.tpr + left.tpr) / 2;
        }
        return aucRoc;
    }

    private static class RateThresholdCurve {

        private final double[] percentiles;
        private final boolean isTp;

        private RateThresholdCurve(double[] percentiles, boolean isTp) {
            this.percentiles = percentiles;
            this.isTp = isTp;
        }

        private double getRate(int index) {
            return 1 - 0.01 * (index + 1);
        }

        private double getThreshold(int index) {
            return percentiles[index];
        }

        private double interpolateRate(double threshold) {
            int binarySearchResult = Arrays.binarySearch(percentiles, threshold);
            if (binarySearchResult >= 0) {
                return getRate(binarySearchResult);
            } else {
                int right = (binarySearchResult * -1) -1;
                int left = right - 1;
                if (right >= percentiles.length) {
                    return 0.0;
                } else if (left < 0) {
                    return 1.0;
                } else {
                    double rightRate = getRate(right);
                    double leftRate = getRate(left);
                    return interpolate(threshold, percentiles[left], leftRate, percentiles[right], rightRate);
                }
            }
        }

        private List<AucRocPoint> scanPoints(RateThresholdCurve againstCurve) {
            List<AucRocPoint> points = new ArrayList<>();
            for (int index = 0; index < percentiles.length; index++) {
                double rate = getRate(index);
                double scannedThreshold = getThreshold(index);
                double againstRate = againstCurve.interpolateRate(scannedThreshold);
                AucRocPoint point;
                if (isTp) {
                    point = new AucRocPoint(rate, againstRate, scannedThreshold);
                } else {
                    point = new AucRocPoint(againstRate, rate, scannedThreshold);
                }
                points.add(point);
            }
            return points;
        }
    }

    public static final class AucRocPoint implements Comparable<AucRocPoint>, ToXContentObject, Writeable {
        double tpr;
        double fpr;
        double threshold;

        private AucRocPoint(double tpr, double fpr, double threshold) {
            this.tpr = tpr;
            this.fpr = fpr;
            this.threshold = threshold;
        }

        private AucRocPoint(StreamInput in) throws IOException {
            this.tpr = in.readDouble();
            this.fpr = in.readDouble();
            this.threshold = in.readDouble();
        }

        @Override
        public int compareTo(AucRocPoint o) {
            return Comparator.comparingDouble((AucRocPoint p) -> p.threshold).reversed()
                .thenComparing(p -> p.fpr)
                .thenComparing(p -> p.tpr)
                .compare(this, o);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(tpr);
            out.writeDouble(fpr);
            out.writeDouble(threshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("tpr", tpr);
            builder.field("fpr", fpr);
            builder.field("threshold", threshold);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private static double interpolate(double x, double x1, double y1, double x2, double y2) {
        return y1 + (x - x1) * (y2 - y1) / (x2 - x1);
    }

    public static class Result implements EvaluationMetricResult {

        private final double score;
        private final List<AucRocPoint> curve;

        public Result(double score, List<AucRocPoint> curve) {
            this.score = score;
            this.curve = Objects.requireNonNull(curve);
        }

        public Result(StreamInput in) throws IOException {
            this.score = in.readDouble();
            this.curve = in.readList(AucRocPoint::new);
        }

        @Override
        public String getWriteableName() {
            return NAME.getPreferredName();
        }

        @Override
        public String getName() {
            return NAME.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(score);
            out.writeList(curve);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("score", score);
            if (curve.isEmpty() == false) {
                builder.field("curve", curve);
            }
            builder.endObject();
            return builder;
        }
    }
}
