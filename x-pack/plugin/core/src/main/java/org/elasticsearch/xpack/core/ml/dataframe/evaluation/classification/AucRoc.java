/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.common.AbstractAucRoc;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

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
public class AucRoc extends AbstractAucRoc {

    public static final ParseField INCLUDE_CURVE = new ParseField("include_curve");
    public static final ParseField CLASS_NAME = new ParseField("class_name");

    public static final ConstructingObjectParser<AucRoc, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(), a -> new AucRoc((Boolean) a[0], (String) a[1]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), INCLUDE_CURVE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CLASS_NAME);
    }

    private static final String TRUE_AGG_NAME = NAME.getPreferredName() + "_true";
    private static final String NON_TRUE_AGG_NAME = NAME.getPreferredName() + "_non_true";
    private static final String NESTED_AGG_NAME = "nested";
    private static final String NESTED_FILTER_AGG_NAME = "nested_filter";
    private static final String PERCENTILES_AGG_NAME = "percentiles";

    public static AucRoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final boolean includeCurve;
    private final String className;
    private final SetOnce<EvaluationFields> fields = new SetOnce<>();
    private final SetOnce<EvaluationMetricResult> result = new SetOnce<>();

    public AucRoc(Boolean includeCurve, String className) {
        this.includeCurve = includeCurve == null ? false : includeCurve;
        this.className = ExceptionsHelper.requireNonNull(className, CLASS_NAME.getPreferredName());
    }

    public AucRoc(StreamInput in) throws IOException {
        this.includeCurve = in.readBoolean();
        this.className = in.readOptionalString();
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(Classification.NAME, NAME);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(includeCurve);
        out.writeOptionalString(className);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INCLUDE_CURVE.getPreferredName(), includeCurve);
        if (className != null) {
            builder.field(CLASS_NAME.getPreferredName(), className);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(
            EvaluationFields.ACTUAL_FIELD.getPreferredName(),
            EvaluationFields.PREDICTED_CLASS_FIELD.getPreferredName(),
            EvaluationFields.PREDICTED_PROBABILITY_FIELD.getPreferredName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AucRoc that = (AucRoc) o;
        return includeCurve == that.includeCurve
            && Objects.equals(className, that.className);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeCurve, className);
    }

    @Override
    public Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                  EvaluationFields fields) {
        if (result.get() != null) {
            return Tuple.tuple(List.of(), List.of());
        }
        // Store given {@code fields} for the purpose of generating error messages in {@code process}.
        this.fields.trySet(fields);

        double[] percentiles = IntStream.range(1, 100).mapToDouble(v -> (double) v).toArray();
        AggregationBuilder percentilesAgg =
            AggregationBuilders
                .percentiles(PERCENTILES_AGG_NAME)
                .field(fields.getPredictedProbabilityField())
                .percentiles(percentiles);
        AggregationBuilder nestedAgg =
            AggregationBuilders
                .nested(NESTED_AGG_NAME, fields.getTopClassesField())
                .subAggregation(
                    AggregationBuilders
                        .filter(NESTED_FILTER_AGG_NAME, QueryBuilders.termQuery(fields.getPredictedClassField(), className))
                        .subAggregation(percentilesAgg));
        QueryBuilder actualIsTrueQuery = QueryBuilders.termQuery(fields.getActualField(), className);
        AggregationBuilder percentilesForClassValueAgg =
            AggregationBuilders
                .filter(TRUE_AGG_NAME, actualIsTrueQuery)
                .subAggregation(nestedAgg);
        AggregationBuilder percentilesForRestAgg =
            AggregationBuilders
                .filter(NON_TRUE_AGG_NAME, QueryBuilders.boolQuery().mustNot(actualIsTrueQuery))
                .subAggregation(nestedAgg);
        return Tuple.tuple(
            List.of(percentilesForClassValueAgg, percentilesForRestAgg),
            List.of());
    }

    @Override
    public void process(Aggregations aggs) {
        if (result.get() != null) {
            return;
        }
        Filter classAgg = aggs.get(TRUE_AGG_NAME);
        Nested classNested = classAgg.getAggregations().get(NESTED_AGG_NAME);
        Filter classNestedFilter = classNested.getAggregations().get(NESTED_FILTER_AGG_NAME);

        Filter restAgg = aggs.get(NON_TRUE_AGG_NAME);
        Nested restNested = restAgg.getAggregations().get(NESTED_AGG_NAME);
        Filter restNestedFilter = restNested.getAggregations().get(NESTED_FILTER_AGG_NAME);

        if (classAgg.getDocCount() == 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] requires at least one [{}] to have the value [{}]",
                getName(), fields.get().getActualField(), className);
        }
        if (restAgg.getDocCount() == 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] requires at least one [{}] to have a different value than [{}]",
                getName(), fields.get().getActualField(), className);
        }
        long filteredDocCount = classNestedFilter.getDocCount() + restNestedFilter.getDocCount();
        long totalDocCount = classAgg.getDocCount() + restAgg.getDocCount();
        if (filteredDocCount < totalDocCount) {
            throw ExceptionsHelper.badRequestException(
                "[{}] requires that [{}] appears as one of the [{}] for every document (appeared in {} out of {}). "
                + "This is probably caused by the {} value being less than the total number of actual classes in the dataset.",
                getName(), className, fields.get().getPredictedClassField(), filteredDocCount, totalDocCount,
                org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification.NUM_TOP_CLASSES.getPreferredName());
        }

        Percentiles classPercentiles = classNestedFilter.getAggregations().get(PERCENTILES_AGG_NAME);
        double[] tpPercentiles = percentilesArray(classPercentiles);
        Percentiles restPercentiles = restNestedFilter.getAggregations().get(PERCENTILES_AGG_NAME);
        double[] fpPercentiles = percentilesArray(restPercentiles);

        List<AucRocPoint> aucRocCurve = buildAucRocCurve(tpPercentiles, fpPercentiles);
        double aucRocScore = calculateAucScore(aucRocCurve);
        result.set(new Result(aucRocScore, includeCurve ? aucRocCurve : Collections.emptyList()));
    }

    @Override
    public Optional<EvaluationMetricResult> getResult() {
        return Optional.ofNullable(result.get());
    }
}
