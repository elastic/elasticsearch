/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfusionMatrix extends AbstractConfusionMatrixMetric {

    public static final ParseField NAME = new ParseField("confusion_matrix");

    private static final ConstructingObjectParser<ConfusionMatrix, Void> PARSER = new ConstructingObjectParser<>(NAME.getPreferredName(),
        a -> new ConfusionMatrix((List<Double>) a[0]));

    static {
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), AT);
    }

    public static ConfusionMatrix fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ConfusionMatrix(List<Double> at) {
        super(at.stream().mapToDouble(Double::doubleValue).toArray());
    }

    public ConfusionMatrix(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getMetricName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfusionMatrix that = (ConfusionMatrix) o;
        return Arrays.equals(thresholds, that.thresholds);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(thresholds);
    }

    @Override
    protected List<AggregationBuilder> aggsAt(String labelField, List<ClassInfo> classInfos, double threshold) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (ClassInfo classInfo : classInfos) {
            aggs.add(buildAgg(classInfo, threshold, Condition.TP));
            aggs.add(buildAgg(classInfo, threshold, Condition.FP));
            aggs.add(buildAgg(classInfo, threshold, Condition.TN));
            aggs.add(buildAgg(classInfo, threshold, Condition.FN));
        }
        return aggs;
    }

    @Override
    public EvaluationMetricResult evaluate(ClassInfo classInfo, Aggregations aggs) {
        long[] tp = new long[thresholds.length];
        long[] fp = new long[thresholds.length];
        long[] tn = new long[thresholds.length];
        long[] fn = new long[thresholds.length];
        for (int i = 0; i < thresholds.length; i++) {
            Filter tpAgg = aggs.get(aggName(classInfo, thresholds[i], Condition.TP));
            Filter fpAgg = aggs.get(aggName(classInfo, thresholds[i], Condition.FP));
            Filter tnAgg = aggs.get(aggName(classInfo, thresholds[i], Condition.TN));
            Filter fnAgg = aggs.get(aggName(classInfo, thresholds[i], Condition.FN));
            tp[i] = tpAgg.getDocCount();
            fp[i] = fpAgg.getDocCount();
            tn[i] = tnAgg.getDocCount();
            fn[i] = fnAgg.getDocCount();
        }
        return new Result(thresholds, tp, fp, tn, fn);
    }

    public static class Result implements EvaluationMetricResult {

        private final double[] thresholds;
        private final long[] tp;
        private final long[] fp;
        private final long[] tn;
        private final long[] fn;

        public Result(double[] thresholds, long[] tp, long[] fp, long[] tn, long[] fn) {
            assert thresholds.length == tp.length;
            assert thresholds.length == fp.length;
            assert thresholds.length == tn.length;
            assert thresholds.length == fn.length;
            this.thresholds = thresholds;
            this.tp = tp;
            this.fp = fp;
            this.tn = tn;
            this.fn = fn;
        }

        public Result(StreamInput in) throws IOException {
            this.thresholds = in.readDoubleArray();
            this.tp = in.readLongArray();
            this.fp = in.readLongArray();
            this.tn = in.readLongArray();
            this.fn = in.readLongArray();
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
            out.writeDoubleArray(thresholds);
            out.writeLongArray(tp);
            out.writeLongArray(fp);
            out.writeLongArray(tn);
            out.writeLongArray(fn);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (int i = 0; i < thresholds.length; i++) {
                builder.startObject(String.valueOf(thresholds[i]));
                builder.field("tp", tp[i]);
                builder.field("fp", fp[i]);
                builder.field("tn", tn[i]);
                builder.field("fn", fn[i]);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }
}
