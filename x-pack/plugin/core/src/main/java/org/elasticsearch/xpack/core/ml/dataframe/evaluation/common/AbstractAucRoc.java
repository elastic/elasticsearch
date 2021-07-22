/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.common;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

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
public abstract class AbstractAucRoc implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("auc_roc");

    protected AbstractAucRoc() {}

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    protected static double[] percentilesArray(Percentiles percentiles) {
        double[] result = new double[99];
        percentiles.forEach(percentile -> {
            if (Double.isNaN(percentile.getValue())) {
                throw ExceptionsHelper.badRequestException(
                    "[{}] requires at all the percentiles values to be finite numbers", NAME.getPreferredName());
            }
            result[((int) percentile.getPercent()) - 1] = percentile.getValue();
        });
        return result;
    }

    /**
     * Visible for testing
     */
    protected static List<AucRocPoint> buildAucRocCurve(double[] tpPercentiles, double[] fpPercentiles) {
        assert tpPercentiles.length == fpPercentiles.length;
        assert tpPercentiles.length == 99;

        List<AucRocPoint> points = new ArrayList<>(tpPercentiles.length + fpPercentiles.length);
        RateThresholdCurve tpCurve = new RateThresholdCurve(tpPercentiles, true);
        RateThresholdCurve fpCurve = new RateThresholdCurve(fpPercentiles, false);
        points.addAll(tpCurve.scanPoints(fpCurve));
        points.addAll(fpCurve.scanPoints(tpCurve));
        Collections.sort(points);

        // As our auc roc curve is comprised by two sets of points coming from two
        // percentiles aggregations, it is possible that we get a non-monotonic result
        // because the percentiles aggregation is an approximation. In order to make
        // our final curve monotonic, we collapse equal threshold points.
        points = collapseEqualThresholdPoints(points);

        List<AucRocPoint> aucRocCurve = new ArrayList<>(points.size() + 2);
        aucRocCurve.add(new AucRocPoint(0.0, 0.0, 1.0));
        aucRocCurve.addAll(points);
        aucRocCurve.add(new AucRocPoint(1.0, 1.0, 0.0));
        return aucRocCurve;
    }

    /**
     * Visible for testing
     *
     * Expects a sorted list of {@link AucRocPoint} points.
     * Collapses points with equal threshold by replacing them
     * with a single point that is the average.
     *
     * @param points A sorted list of {@link AucRocPoint} points
     * @return a new list of points where equal threshold points have been collapsed into their average
     */
    static List<AucRocPoint> collapseEqualThresholdPoints(List<AucRocPoint> points) {
        List<AucRocPoint> collapsed = new ArrayList<>();
        List<AucRocPoint> equalThresholdPoints = new ArrayList<>();
        for (AucRocPoint point : points) {
            if (equalThresholdPoints.isEmpty() == false && equalThresholdPoints.get(0).threshold != point.threshold) {
                collapsed.add(calculateAveragePoint(equalThresholdPoints));
                equalThresholdPoints = new ArrayList<>();
            }
            equalThresholdPoints.add(point);
        }

        if (equalThresholdPoints.isEmpty() == false) {
            collapsed.add(calculateAveragePoint(equalThresholdPoints));
        }

        return collapsed;
    }

    private static AucRocPoint calculateAveragePoint(List<AucRocPoint> points) {
        if (points.isEmpty()) {
            throw new IllegalArgumentException("points must not be empty");
        }

        if (points.size() == 1) {
            return points.get(0);
        }

        double avgTpr = 0.0;
        double avgFpr = 0.0;
        double avgThreshold = 0.0;
        for (AucRocPoint sameThresholdPoint : points) {
            avgTpr += sameThresholdPoint.tpr;
            avgFpr += sameThresholdPoint.fpr;
            avgThreshold += sameThresholdPoint.threshold;
        }

        int n = points.size();
        return new AucRocPoint(avgTpr / n, avgFpr / n, avgThreshold / n);
    }

    /**
     * Visible for testing
     */
    protected static double calculateAucScore(List<AucRocPoint> rocCurve) {
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
            // We subtract the minimum value possible here in order to
            // ensure no point has a threshold of 1.0 as we are adding
            // that point separately so that fpr = tpr = 0.
            return Math.max(0, percentiles[index] - Math.ulp(percentiles[index]));
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

        private static final String TPR = "tpr";
        private static final String FPR = "fpr";
        private static final String THRESHOLD = "threshold";

        final double tpr;
        final double fpr;
        final double threshold;

        public AucRocPoint(double tpr, double fpr, double threshold) {
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
            builder.field(TPR, tpr);
            builder.field(FPR, fpr);
            builder.field(THRESHOLD, threshold);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AucRocPoint that = (AucRocPoint) o;
            return tpr == that.tpr
                && fpr == that.fpr
                && threshold == that.threshold;
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

    private static double interpolate(double x, double x1, double y1, double x2, double y2) {
        return y1 + (x - x1) * (y2 - y1) / (x2 - x1);
    }

    public static class Result implements EvaluationMetricResult {

        public static final String NAME = "auc_roc_result";

        private static final String VALUE = "value";
        private static final String CURVE = "curve";

        private final double value;
        private final List<AucRocPoint> curve;

        public Result(double value, List<AucRocPoint> curve) {
            this.value = value;
            this.curve = Objects.requireNonNull(curve);
        }

        public Result(StreamInput in) throws IOException {
            this.value = in.readDouble();
            this.curve = in.readList(AucRocPoint::new);
        }

        public double getValue() {
            return value;
        }

        public List<AucRocPoint> getCurve() {
            return Collections.unmodifiableList(curve);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public String getMetricName() {
            return AbstractAucRoc.NAME.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(value);
            out.writeList(curve);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(VALUE, value);
            if (curve.isEmpty() == false) {
                builder.field(CURVE, curve);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return value == that.value
                && Objects.equals(curve, that.curve);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, curve);
        }
    }
}
