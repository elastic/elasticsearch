/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class ClassificationTests extends AbstractXContentTestCase<Classification> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static Classification createRandom() {
        List<EvaluationMetric> metrics =
            randomSubsetOf(
                Arrays.asList(
                    AucRocMetricTests.createRandom(),
                    AccuracyMetricTests.createRandom(),
                    PrecisionMetricTests.createRandom(),
                    RecallMetricTests.createRandom(),
                    MulticlassConfusionMatrixMetricTests.createRandom()));
        return new Classification(
            randomAlphaOfLength(10),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null,
            metrics.isEmpty() ? null : metrics);
    }

    @Override
    protected Classification createTestInstance() {
        return createRandom();
    }

    @Override
    protected Classification doParseInstance(XContentParser parser) throws IOException {
        return Classification.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }
}
