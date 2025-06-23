/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.sameInstance;

public class InternalInferenceAggregationTests extends InternalAggregationTestCase<InternalInferenceAggregation> {

    @Override
    protected SearchPlugin registerPlugin() {
        return MachineLearningTests.createTrialLicensedMachineLearning(Settings.EMPTY);
    }

    @Override
    protected InternalInferenceAggregation createTestInstance(String name, Map<String, Object> metadata) {
        InferenceResults result;

        if (randomBoolean()) {
            // build a random result with the result field set to `value`
            ClassificationInferenceResults randomResults = ClassificationInferenceResultsTests.createRandomResults();
            result = new ClassificationInferenceResults(
                randomResults.value(),
                randomResults.getClassificationLabel(),
                randomResults.getTopClasses(),
                randomResults.getFeatureImportance(),
                new ClassificationConfig(null, "value", null, null, randomResults.getPredictionFieldType()),
                randomResults.getPredictionProbability(),
                randomResults.getPredictionScore()
            );
        } else if (randomBoolean()) {
            // build a random result with the result field set to `value`
            RegressionInferenceResults randomResults = RegressionInferenceResultsTests.createRandomResults();
            result = new RegressionInferenceResults(randomResults.value(), "value", randomResults.getFeatureImportance());
        } else {
            result = new WarningInferenceResults("this is a warning");
        }

        return new InternalInferenceAggregation(name, metadata, result);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).getReducer(null, 0));
    }

    @Override
    protected void assertReduced(InternalInferenceAggregation reduced, List<InternalInferenceAggregation> inputs) {
        // no test since reduce operation is unsupported
    }

    public void testGetProperty_givenEmptyPath() {
        InternalInferenceAggregation internalAgg = createTestInstance();
        assertThat(internalAgg, sameInstance(internalAgg.getProperty(Collections.emptyList())));
    }

    public void testGetProperty_givenTooLongPath() {
        InternalInferenceAggregation internalAgg = createTestInstance();
        InvalidAggregationPathException e = expectThrows(
            InvalidAggregationPathException.class,
            () -> internalAgg.getProperty(Arrays.asList("one", "two"))
        );

        String message = "unknown property [one, two] for inference aggregation [" + internalAgg.getName() + "]";
        assertEquals(message, e.getMessage());
    }

    public void testGetProperty_givenWrongPath() {
        InternalInferenceAggregation internalAgg = createTestInstance();
        InvalidAggregationPathException e = expectThrows(
            InvalidAggregationPathException.class,
            () -> internalAgg.getProperty(Collections.singletonList("bar"))
        );

        String message = "unknown property [bar] for inference aggregation [" + internalAgg.getName() + "]";
        assertEquals(message, e.getMessage());
    }

    public void testGetProperty_value() {
        {
            ClassificationInferenceResults results = ClassificationInferenceResultsTests.createRandomResults();
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            assertEquals(results.predictedValue(), internalAgg.getProperty(Collections.singletonList("value")));
        }

        {
            RegressionInferenceResults results = RegressionInferenceResultsTests.createRandomResults();
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            assertEquals(results.value(), internalAgg.getProperty(Collections.singletonList("value")));
        }

        {
            WarningInferenceResults results = new WarningInferenceResults("a warning from history");
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            assertNull(internalAgg.getProperty(Collections.singletonList("value")));
        }
    }

    public void testGetProperty_featureImportance() {
        {
            ClassificationInferenceResults results = ClassificationInferenceResultsTests.createRandomResults();
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            expectThrows(
                InvalidAggregationPathException.class,
                () -> internalAgg.getProperty(Collections.singletonList("feature_importance"))
            );
        }

        {
            RegressionInferenceResults results = RegressionInferenceResultsTests.createRandomResults();
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            expectThrows(
                InvalidAggregationPathException.class,
                () -> internalAgg.getProperty(Collections.singletonList("feature_importance"))
            );
        }

        {
            WarningInferenceResults results = new WarningInferenceResults("a warning from history");
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            expectThrows(
                InvalidAggregationPathException.class,
                () -> internalAgg.getProperty(Collections.singletonList("feature_importance"))
            );
        }
    }

    public void testGetProperty_topClasses() {
        {
            ClassificationInferenceResults results = ClassificationInferenceResultsTests.createRandomResults();
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            expectThrows(InvalidAggregationPathException.class, () -> internalAgg.getProperty(Collections.singletonList("top_classes")));
        }

        {
            RegressionInferenceResults results = RegressionInferenceResultsTests.createRandomResults();
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            expectThrows(InvalidAggregationPathException.class, () -> internalAgg.getProperty(Collections.singletonList("top_classes")));
        }

        {
            WarningInferenceResults results = new WarningInferenceResults("a warning from history");
            InternalInferenceAggregation internalAgg = new InternalInferenceAggregation("foo", Collections.emptyMap(), results);
            expectThrows(InvalidAggregationPathException.class, () -> internalAgg.getProperty(Collections.singletonList("top_classes")));
        }
    }

    @Override
    protected InternalInferenceAggregation mutateInstance(InternalInferenceAggregation instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
