/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalInferenceAggregationTests extends InternalAggregationTestCase<InternalInferenceAggregation> {

    protected SearchPlugin registerPlugin() {
        return new MachineLearning(Settings.EMPTY, null);
    }

    @Override
    protected InternalInferenceAggregation createTestInstance(String name, Map<String, Object> metadata) {

        InferenceResults result = randomBoolean() ?
            ClassificationInferenceResultsTests.createRandomResults()
            : RegressionInferenceResultsTests.createRandomResults();

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
    protected void assertFromXContent(InternalInferenceAggregation aggregation, ParsedAggregation parsedAggregation) throws IOException {

    }
}
