/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.sameInstance;

public class InternalInferenceAggregationTests extends InternalAggregationTestCase<InternalInferenceAggregation> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new MachineLearning(Settings.EMPTY);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField(InferencePipelineAggregationBuilder.NAME),
                (p, c) -> ParsedInference.fromXContent(p, (String) c)
            )
        );
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
        if (result instanceof WarningInferenceResults warning) {
            assertEquals(warning.getWarning(), parsed.getWarning());
        } else if (result instanceof RegressionInferenceResults regression) {
            assertEquals(regression.value(), parsed.getValue());
            List<RegressionFeatureImportance> featureImportance = regression.getFeatureImportance();
            if (featureImportance.isEmpty()) {
                featureImportance = null;
            }
            assertEquals(featureImportance, parsed.getFeatureImportance());
        } else if (result instanceof ClassificationInferenceResults classification) {
            assertEquals(classification.predictedValue(), parsed.getValue());

            List<ClassificationFeatureImportance> featureImportance = classification.getFeatureImportance();
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
