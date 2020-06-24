/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.FeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class InternalInferenceAggregationTests extends InternalAggregationTestCase<InternalInferenceAggregation> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new MachineLearning(Settings.EMPTY, null);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(super.getNamedXContents());
        entries.add(new NamedXContentRegistry.Entry(Aggregation.class,
            new ParseField(InferencePipelineAggregationBuilder.NAME), (p, c) -> ParsedInference.fromXContent(p, (String)c)));

        return entries;
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return p -> p.contains("top_classes") || p.contains("feature_importance");
    }

    @Override
    protected InternalInferenceAggregation createTestInstance(String name, Map<String, Object> metadata) {
        InferenceResults result;

        if (randomBoolean()) {
            // build a random result with the result field set to `value`
            ClassificationInferenceResults randomResults = ClassificationInferenceResultsTests.createRandomResults();
            Double value = randomResults.value();
            PredictionFieldType predictionFieldType = randomFrom(PredictionFieldType.values());
            if (predictionFieldType == PredictionFieldType.BOOLEAN) {
                // value must be close to 0 or 1
                value = randomBoolean() ? 0.0 : 1.0;
            }
            result = new ClassificationInferenceResults(
                value,
                randomResults.getClassificationLabel(),
                randomResults.getTopClasses(),
                randomResults.getFeatureImportance(),
                new ClassificationConfig(null, "value", null, null, predictionFieldType)
            );
        } else if (randomBoolean()) {
            // build a random result with the result field set to `value`
            RegressionInferenceResults randomResults = RegressionInferenceResultsTests.createRandomResults();
            result = new RegressionInferenceResults(
                randomResults.value(),
                "value",
                randomResults.getFeatureImportance());
        } else {
            result = new WarningInferenceResults("this is a warning");
        }

        return new InternalInferenceAggregation(name, metadata, result);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalInferenceAggregation reduced, List<InternalInferenceAggregation> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected void assertFromXContent(InternalInferenceAggregation agg, ParsedAggregation parsedAggregation) {
        ParsedInference parsed = ((ParsedInference) parsedAggregation);

        InferenceResults result = agg.getInferenceResult();
        if (result instanceof WarningInferenceResults) {
            WarningInferenceResults warning = (WarningInferenceResults) result;
            assertEquals(warning.getWarning(), parsed.getWarning());
        } else if (result instanceof RegressionInferenceResults) {
            RegressionInferenceResults regression = (RegressionInferenceResults) result;
            assertEquals(regression.value(), parsed.getValue());
            List<FeatureImportance> featureImportance = regression.getFeatureImportance();
            if (featureImportance.isEmpty()) {
                featureImportance = null;
            }
            assertEquals(featureImportance, parsed.getFeatureImportance());
        } else if (result instanceof ClassificationInferenceResults) {
            ClassificationInferenceResults classification = (ClassificationInferenceResults) result;
            assertEquals(classification.transformedPredictedValue(), parsed.getValue());

            List<FeatureImportance> featureImportance = classification.getFeatureImportance();
            if (featureImportance.isEmpty()) {
                featureImportance = null;
            }
            assertEquals(featureImportance, parsed.getFeatureImportance());

            List<TopClassEntry> topClasses = classification.getTopClasses();
            if (topClasses.isEmpty()) {
                topClasses = null;
            }
            assertEquals(topClasses, parsed.getTopClasses());
        }
    }
}
